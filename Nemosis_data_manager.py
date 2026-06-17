import os
import shutil
import pandas as pd
import json
import re
import zipfile
from datetime import datetime, timedelta
from dotenv import load_dotenv
from urllib.parse import urljoin
from urllib.request import urlopen

try:
    from nemosis import dynamic_data_compiler
    from nemosis.data_fetch_methods import NoDataToReturn
except ImportError:
    dynamic_data_compiler = None

    class NoDataToReturn(Exception):
        pass


class NEMDispatchLoadManager:
    """
    使用 nemosis 获取 AEMO dispatch 数据并存为 parquet。
    """

    def __init__(
            self,
            storage_dir=None,
            table_name="DISPATCHLOAD",
            raw_cache_dir=None,
            env_key="STORAGE_DIR",
            interval_minutes=5,
            overlap_hours=24,
            primary_key=None
    ):
        load_dotenv()

        storage_dir = storage_dir or os.getenv(env_key)
        if not storage_dir:
            raise ValueError(
                f"Storage directory is not configured. Pass storage_dir or set {env_key} in .env."
            )

        self.api_name = "nemosis"
        self.table_name = table_name
        self.interval_minutes = interval_minutes
        self.interval_str = f"{interval_minutes}min"
        self.overlap = timedelta(hours=overlap_hours)
        self.primary_key = primary_key or self._default_primary_key(table_name)
        storage_dir = os.path.expanduser(storage_dir)

        # 临时缓存目录
        if not raw_cache_dir:
            self.raw_cache_dir = os.path.join(storage_dir, self.interval_str, "nemo_cache")
        else:
            self.raw_cache_dir = os.path.expanduser(raw_cache_dir)
        os.makedirs(self.raw_cache_dir, exist_ok=True)

        # 最终 Parquet 存储目录
        self.storage_dir = os.path.join(storage_dir, self.interval_str)
        os.makedirs(self.storage_dir, exist_ok=True)

        self.metadata_path = os.path.join(
            self.storage_dir,
            f"metadata_{self.api_name}_{self.table_name}_{self.interval_str}.json"
        )
        self.metadata = self._load_metadata()

    @staticmethod
    def _default_primary_key(table_name):
        table_keys = {
            "DISPATCHLOAD": ["SETTLEMENTDATE", "DUID", "INTERVENTION"],
        }
        return table_keys.get(table_name.upper(), ["SETTLEMENTDATE"])

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_metadata(self, final_df):
        if final_df.empty:
            return

        time_series = self._get_time_series(final_df)
        payload = {
            "api": self.api_name,
            "table": self.table_name,
            "rows": int(len(final_df)),
            "start_time": str(time_series.min()),
            "end_time": str(time_series.max()),
            "updated_at": datetime.utcnow().isoformat(),
            "file_path": self._get_file_path(),
            "primary_key": self.primary_key,
            "overlap_hours": self.overlap.total_seconds() / 3600
        }
        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)
        self.metadata = payload

    def _get_file_path(self):
        return os.path.join(
            self.storage_dir,
            f"{self.api_name}_{self.table_name}_{self.interval_str}.parquet"
        )

    def _cleanup_temp_files(self):
        """
        清空临时文件夹中的所有文件和子目录
        """
        print(f"正在清理临时目录: {self.raw_cache_dir}")
        for filename in os.listdir(self.raw_cache_dir):
            file_path = os.path.join(self.raw_cache_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # 删除文件或链接
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # 删除子目录
            except Exception as e:
                print(f"清理文件 {file_path} 失败，原因: {e}")

    def _get_time_series(self, df):
        if "SETTLEMENTDATE" in df.columns:
            return pd.to_datetime(df["SETTLEMENTDATE"])
        if isinstance(df.index, pd.DatetimeIndex):
            return pd.Series(df.index, index=df.index)
        raise ValueError("Data does not contain SETTLEMENTDATE or a DatetimeIndex.")

    def _normalise_dataframe(self, df):
        if df.empty:
            return df

        normalised = df.copy()
        if "SETTLEMENTDATE" not in normalised.columns and isinstance(normalised.index, pd.DatetimeIndex):
            normalised = normalised.reset_index()
            if "index" in normalised.columns:
                normalised = normalised.rename(columns={"index": "SETTLEMENTDATE"})

        if "SETTLEMENTDATE" in normalised.columns:
            normalised["SETTLEMENTDATE"] = pd.to_datetime(normalised["SETTLEMENTDATE"])
            normalised = normalised.sort_values("SETTLEMENTDATE")

        return normalised

    def _dedupe_dataframe(self, df):
        available_key = [col for col in self.primary_key if col in df.columns]
        if available_key:
            return df.drop_duplicates(subset=available_key, keep="last")
        return df.drop_duplicates(keep="last")

    def update_data(self, start_time="2024/01/01 00:00:00", end_time=None, cleanup=True):
        """
        更新数据并可选是否清理临时文件
        """
        if dynamic_data_compiler is None:
            raise ImportError("nemosis is not installed in the current Python environment.")

        file_path = self._get_file_path()

        # 1. 增量时间判定
        if os.path.exists(file_path):
            existing_df = self._normalise_dataframe(pd.read_parquet(file_path))
            last_ts = self._get_time_series(existing_df).max()
            fetch_start = max(
                pd.to_datetime(start_time),
                last_ts - self.overlap
            ).strftime("%Y/%m/%d %H:%M:%S")
        else:
            existing_df = pd.DataFrame()
            fetch_start = start_time

        if end_time is None:
            end_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        if pd.to_datetime(fetch_start) >= pd.to_datetime(end_time):
            print("本地数据已经覆盖请求区间。")
            return existing_df

        # 2. 调用 nemosis 下载
        print(f"正在从 AEMO 下载 {self.table_name}: {fetch_start} -> {end_time}")
        try:
            new_df = dynamic_data_compiler(
                start_time=fetch_start,
                end_time=end_time,
                table_name=self.table_name,
                raw_data_location=self.raw_cache_dir
            )
        except NoDataToReturn:
            print("nemosis 未返回新数据。")
            if cleanup:
                self._cleanup_temp_files()
            return existing_df

        if new_df is None or new_df.empty:
            print("未获取到新数据。")
            if cleanup:
                self._cleanup_temp_files()
            return existing_df

        # 3. 处理索引与合并
        new_df = self._normalise_dataframe(new_df)

        if not existing_df.empty:
            final_df = pd.concat([existing_df, new_df])
        else:
            final_df = new_df

        final_df = self._dedupe_dataframe(final_df)
        final_df = self._normalise_dataframe(final_df)

        # 4. 保存核心 Parquet 文件
        final_df.to_parquet(file_path, engine='pyarrow', compression='snappy')
        self._save_metadata(final_df)
        print(f"Parquet 已更新: {file_path}")

        # 5. 执行清理 (关键改动)
        if cleanup:
            self._cleanup_temp_files()

        return final_df


class CurrentNEMWebFetcher:
    """
    从 NEMWeb CURRENT 目录抓取最新快照，并将每次运行的原始文件归档。
    """

    CURRENT_BASE_URL = "https://www.nemweb.com.au/REPORTS/CURRENT/"
    REPORT_PATHS = {
        "dispatch_is": "DispatchIS_Reports/",
        "dispatch_scada": "Dispatch_SCADA/",
        "public_prices": "Public_Prices/",
        "dispatch_reports": "Dispatch_Reports/",
    }

    def __init__(
            self,
            storage_dir=None,
            env_key="STORAGE_DIR",
            archive_dir="current_archive",
            base_url=None
    ):
        load_dotenv()

        storage_dir = storage_dir or os.getenv(env_key)
        if not storage_dir:
            raise ValueError(
                f"Storage directory is not configured. Pass storage_dir or set {env_key} in .env."
            )

        self.storage_dir = os.path.expanduser(storage_dir)
        self.archive_root = os.path.join(self.storage_dir, archive_dir)
        self.base_url = base_url or self.CURRENT_BASE_URL
        os.makedirs(self.archive_root, exist_ok=True)

    def list_files(self, report_type):
        report_url = self._get_report_url(report_type)
        with urlopen(report_url) as response:
            html = response.read().decode("utf-8", errors="ignore")

        hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
        files = [
            urljoin(report_url, href)
            for href in hrefs
            if href.lower().endswith((".zip", ".csv"))
        ]
        return sorted(set(files))

    def fetch_latest(self, report_type, keep_zip=True):
        files = self.list_files(report_type)
        if not files:
            raise FileNotFoundError(f"No current NEMWeb files found for report_type={report_type}.")

        source_url = files[-1]
        return self.fetch_file(report_type=report_type, source_url=source_url, keep_zip=keep_zip)

    def fetch_file(self, report_type, source_url, keep_zip=True):
        downloaded_at = datetime.utcnow()
        snapshot_dir = os.path.join(
            self.archive_root,
            report_type,
            downloaded_at.strftime("%Y%m%d_%H%M%S")
        )
        os.makedirs(snapshot_dir, exist_ok=True)

        source_name = os.path.basename(source_url)
        raw_path = os.path.join(snapshot_dir, source_name)
        with urlopen(source_url) as response:
            raw_bytes = response.read()

        with open(raw_path, "wb") as f:
            f.write(raw_bytes)

        dataframes = self._read_payload(raw_path, snapshot_dir)
        df = pd.concat(dataframes, ignore_index=True) if len(dataframes) > 1 else dataframes[0]

        parquet_path = os.path.join(snapshot_dir, "snapshot.parquet")
        df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

        if raw_path.lower().endswith(".zip") and not keep_zip:
            os.unlink(raw_path)

        metadata = {
            "report_type": report_type,
            "source_url": source_url,
            "source_name": source_name,
            "downloaded_at": downloaded_at.isoformat(),
            "rows": int(len(df)),
            "columns": list(df.columns),
            "snapshot_dir": snapshot_dir,
            "parquet_path": parquet_path,
        }
        metadata_path = os.path.join(snapshot_dir, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)

        return df, metadata

    def _get_report_url(self, report_type):
        if report_type in self.REPORT_PATHS:
            return urljoin(self.base_url, self.REPORT_PATHS[report_type])
        if report_type.startswith("http"):
            return report_type
        return urljoin(self.base_url, report_type.strip("/") + "/")

    def _read_payload(self, raw_path, snapshot_dir):
        if raw_path.lower().endswith(".zip"):
            dataframes = []
            with zipfile.ZipFile(raw_path) as archive:
                for member in archive.namelist():
                    if member.lower().endswith(".csv"):
                        csv_path = os.path.join(snapshot_dir, os.path.basename(member))
                        with archive.open(member) as source, open(csv_path, "wb") as target:
                            target.write(source.read())
                        dataframes.append(self._read_csv(csv_path))
            if not dataframes:
                raise ValueError(f"No CSV files found in {raw_path}.")
            return dataframes

        if raw_path.lower().endswith(".csv"):
            return [self._read_csv(raw_path)]

        raise ValueError(f"Unsupported NEMWeb file type: {raw_path}")

    @staticmethod
    def _read_csv(csv_path):
        raw = pd.read_csv(csv_path, header=None, dtype=str)
        first_col = set(raw.iloc[:, 0].dropna().astype(str).str.upper().unique())
        if {"I", "D"}.issubset(first_col):
            return CurrentNEMWebFetcher._read_aemo_mms_csv(raw)

        return pd.read_csv(csv_path)

    @staticmethod
    def _read_aemo_mms_csv(raw):
        rows = []
        current_header = None

        for row in raw.itertuples(index=False, name=None):
            record_type = str(row[0]).upper() if row[0] is not None else ""
            values = [value for value in row if pd.notna(value)]

            if record_type == "I":
                current_header = values
            elif record_type == "D" and current_header:
                padded_values = values + [None] * max(0, len(current_header) - len(values))
                rows.append(dict(zip(current_header, padded_values[:len(current_header)])))

        if not rows:
            return raw

        return pd.DataFrame(rows)


class NEMDispatchModelAdapter:
    """
    将 historical/current dispatch DataFrame 转成 Pyomo 模型更容易消费的集合和参数。
    """

    COLUMN_ALIASES = {
        "time": ["SETTLEMENTDATE", "SETTLEMENT_DATE", "INTERVAL_DATETIME", "DATETIME"],
        "generator": ["DUID", "UNIT", "GENERATOR"],
        "region": ["REGIONID", "REGION_ID", "REGION"],
        "dispatch_mw": ["TOTALCLEARED", "INITIALMW", "SCADAVALUE", "MW"],
        "availability_mw": ["AVAILABILITY", "AVAILABLECAPACITY", "MAXAVAIL"],
    }

    def __init__(self, column_aliases=None):
        aliases = {key: list(value) for key, value in self.COLUMN_ALIASES.items()}
        if column_aliases:
            for key, value in column_aliases.items():
                aliases[key] = list(value) + aliases.get(key, [])
        self.column_aliases = aliases

    def to_dispatch_input(self, df):
        canonical_df = self.to_canonical_dispatch_df(df)

        time_periods = sorted(canonical_df["time"].dropna().unique())
        generators = sorted(canonical_df["generator"].dropna().unique())

        dispatch_mw = self._series_to_param(canonical_df, "dispatch_mw")
        availability_mw = self._series_to_param(canonical_df, "availability_mw")

        return {
            "time_periods": time_periods,
            "generators": generators,
            "dispatch_mw": dispatch_mw,
            "availability_mw": availability_mw,
            "canonical_df": canonical_df,
        }

    def to_canonical_dispatch_df(self, df):
        canonical = pd.DataFrame()

        time_col = self._find_column(df, "time", required=True)
        generator_col = self._find_column(df, "generator", required=True)
        canonical["time"] = pd.to_datetime(df[time_col])
        canonical["generator"] = df[generator_col].astype(str)

        region_col = self._find_column(df, "region")
        if region_col:
            canonical["region"] = df[region_col].astype(str)

        for canonical_name in ["dispatch_mw", "availability_mw"]:
            source_col = self._find_column(df, canonical_name)
            if source_col:
                canonical[canonical_name] = pd.to_numeric(df[source_col], errors="coerce")

        canonical = canonical.sort_values(["time", "generator"])
        canonical = canonical.drop_duplicates(subset=["time", "generator"], keep="last")
        return canonical

    def _find_column(self, df, canonical_name, required=False):
        upper_to_original = {column.upper(): column for column in df.columns}
        for candidate in self.column_aliases.get(canonical_name, []):
            original = upper_to_original.get(candidate.upper())
            if original:
                return original

        if required:
            raise ValueError(
                f"Cannot map required column '{canonical_name}'. Available columns: {list(df.columns)}"
            )
        return None

    @staticmethod
    def _series_to_param(canonical_df, value_col):
        if value_col not in canonical_df.columns:
            return {}

        values = canonical_df.dropna(subset=[value_col])
        return {
            (row.generator, row.time): float(getattr(row, value_col))
            for row in values.itertuples(index=False)
        }



# 使用示例
if __name__ == "__main__":
    manager = NEMDispatchLoadManager()

    # 第一次运行会下载 1 小时数据并保存
    # 第二次运行会自动从上次结束的时间开始往后续传
    df = manager.update_data(
        start_time="2024/01/01 00:00:00",
        end_time="2024/01/01 05:00:00"
    )
