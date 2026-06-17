import os
import shutil
import pandas as pd
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

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



# 使用示例
if __name__ == "__main__":
    manager = NEMDispatchLoadManager()

    # 第一次运行会下载 1 小时数据并保存
    # 第二次运行会自动从上次结束的时间开始往后续传
    df = manager.update_data(
        start_time="2024/01/01 00:00:00",
        end_time="2024/01/01 05:00:00"
    )
