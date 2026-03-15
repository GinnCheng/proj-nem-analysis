import os
import pandas as pd
import json
import sqlite3
from datetime import datetime
from nempy.historical_inputs import mms_db


class NEMPriceDataManager:
    """
    用 nempy/AEMO MMS 获取 NEM 区域电价数据，并保存为 parquet + metadata

    支持区域:
    - NSW1
    - QLD1
    - VIC1
    - SA1
    - TAS1

    文件命名示例:
    - parquet:  nempy_NSW1_5min.parquet
    - metadata: metadata_nempy_NSW1_5min.json

    目录结构示例:
    storage_dir/
        5m/
            nempy_NSW1_5min.parquet
            metadata_nempy_NSW1_5min.json
    """

    def __init__(
        self,
        region="NSW1",
        db_path=r"/Users/gzcheng/Desktop/Udacity/_history_data_for_investment/nempy/historical_mms.db",
        storage_dir=r"/Users/gzcheng/Desktop/Udacity/_history_data_for_investment",
        interval="5m",
    ):
        allowed_regions = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
        if region not in allowed_regions:
            raise ValueError(f"region 必须是 {allowed_regions}")

        if interval != "5m":
            raise ValueError("当前这个 class 只支持 5m，因为 NEM dispatch price 是 5 分钟级别")

        self.region = region
        self.interval = interval
        self.api_name = "nempy"
        self.interval_folder = "5m"
        self.interval_str = "5min"

        self.db_path = db_path
        self.base_storage_dir = storage_dir
        self.storage_dir = os.path.join(storage_dir, self.interval_str)
        os.makedirs(self.storage_dir, exist_ok=True)

        self.metadata_path = os.path.join(
            self.storage_dir,
            f"metadata_{self.api_name}_{self.region}_{self.interval_str}.json"
        )
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_metadata(self, final_df, extra_info=None):
        payload = {
            "api": self.api_name,
            "region": self.region,
            "interval": self.interval,
            "interval_str": self.interval_str,
            "rows": int(len(final_df)) if not final_df.empty else 0,
            "start_utc": str(final_df.index[0]) if not final_df.empty else None,
            "end_utc": str(final_df.index[-1]) if not final_df.empty else None,
            "last_update_utc": str(final_df.index[-1]) if not final_df.empty else None,
            "updated_at": datetime.utcnow().isoformat(),
        }

        if extra_info:
            payload.update(extra_info)

        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)

        self.metadata = payload

    def _get_file_path(self):
        return os.path.join(
            self.storage_dir,
            f"{self.api_name}_{self.region}_{self.interval_str}.parquet"
        )

    def _populate_mms_if_needed(self, start_year, start_month, end_year, end_month):
        """
        用 nempy 的 DBManager 把 AEMO MMS 数据下载到本地 sqlite。
        """
        con = sqlite3.connect(self.db_path)
        try:
            db_manager = mms_db.DBManager(connection=con)
            db_manager.populate(
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month
            )
        finally:
            con.close()

    def _fetch_price_range(self, start_dt, end_dt):
        """
        直接从本地 sqlite 的 DISPATCHPRICE 表读取区域电价。
        """
        if start_dt >= end_dt:
            return pd.DataFrame()

        con = sqlite3.connect(self.db_path)

        query = """
        SELECT
            SETTLEMENTDATE,
            REGIONID,
            RRP,
            ROP,
            RAISE6SECRRP,
            RAISE60SECRRP,
            RAISE5MINRRP,
            RAISE60MINRRP,
            LOWER6SECRRP,
            LOWER60SECRRP,
            LOWER5MINRRP,
            LOWER60MINRRP
        FROM DISPATCHPRICE
        WHERE REGIONID = ?
          AND SETTLEMENTDATE >= ?
          AND SETTLEMENTDATE <= ?
        ORDER BY SETTLEMENTDATE
        """

        try:
            df = pd.read_sql_query(
                query,
                con,
                params=(
                    self.region,
                    start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
        finally:
            con.close()

        if df.empty:
            return pd.DataFrame()

        df["datetime"] = pd.to_datetime(df["SETTLEMENTDATE"], utc=True)
        df = df.rename(columns={
            "REGIONID": "region",
            "RRP": "rrp",
            "ROP": "rop",
            "RAISE6SECRRP": "raise_6sec_rrp",
            "RAISE60SECRRP": "raise_60sec_rrp",
            "RAISE5MINRRP": "raise_5min_rrp",
            "RAISE60MINRRP": "raise_60min_rrp",
            "LOWER6SECRRP": "lower_6sec_rrp",
            "LOWER60SECRRP": "lower_60sec_rrp",
            "LOWER5MINRRP": "lower_5min_rrp",
            "LOWER60MINRRP": "lower_60min_rrp",
        })

        keep_cols = [
            "datetime",
            "region",
            "rrp",
            "rop",
            "raise_6sec_rrp",
            "raise_60sec_rrp",
            "raise_5min_rrp",
            "raise_60min_rrp",
            "lower_6sec_rrp",
            "lower_60sec_rrp",
            "lower_5min_rrp",
            "lower_60min_rrp",
        ]
        df = df[keep_cols].copy()
        df.set_index("datetime", inplace=True)
        df = df.sort_index()

        numeric_cols = [c for c in df.columns if c != "region"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

        return df

    def update_data(
        self,
        start_year=2020,
        start_month=1,
        end_year=None,
        end_month=None,
        auto_populate=True,
    ):
        """
        默认逻辑:
        1. 先确保 nempy MMS 本地数据库有数据
        2. 从 DISPATCHPRICE 读取指定区域
        3. 如果已有 parquet，就增量拼接
        """

        now = datetime.utcnow()
        if end_year is None:
            end_year = now.year
        if end_month is None:
            end_month = now.month

        file_path = self._get_file_path()

        if auto_populate:
            print(f"开始用 nempy/AEMO MMS 补充数据库: {start_year}-{start_month} -> {end_year}-{end_month}")
            self._populate_mms_if_needed(start_year, start_month, end_year, end_month)

        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path)
            existing_df.index = pd.to_datetime(existing_df.index, utc=True)
            existing_df = existing_df.sort_index()

            start_dt = existing_df.index[-1].to_pydatetime()
            print(f"检测到本地数据：{self.region} {self.interval}，从 {start_dt} 开始增量更新...")
        else:
            existing_df = pd.DataFrame()
            start_dt = datetime(start_year, start_month, 1)
            print(f"本地无数据，开始获取 {self.region} {self.interval} 历史全量数据 from {start_dt}...")

        # 这里给到月底后一点缓冲，不纠结月末天数
        if end_month == 12:
            end_dt = datetime(end_year + 1, 1, 1)
        else:
            end_dt = datetime(end_year, end_month + 1, 1)

        new_df = self._fetch_price_range(start_dt, end_dt)

        if new_df.empty and not existing_df.empty:
            print(f"{self.region} {self.interval} 已经是最新状态。")
            return existing_df

        final_df = pd.concat([existing_df, new_df]) if not existing_df.empty else new_df
        final_df = final_df[~final_df.index.duplicated(keep="last")].sort_index()

        if final_df.empty:
            print(f"{self.region} {self.interval} 没有抓到数据。")
            return final_df

        final_df.to_parquet(file_path)

        extra_info = {
            "db_path": self.db_path,
            "table": "DISPATCHPRICE",
            "parquet_path": file_path,
            "metadata_path": self.metadata_path,
        }
        self._save_metadata(final_df, extra_info=extra_info)

        print(f"更新完成：{self.region} {self.interval}")
        print(f"当前本地数据区间：{final_df.index[0]} -> {final_df.index[-1]}")
        print(f"共 {len(final_df)} 行")
        print(f"保存文件：{file_path}")

        return final_df


# how to use
# manager = NEMPriceDataManager(
#     region="NSW1",
#     db_path="path/historical_mms.db",
#     storage_dir="path"
# )
#
# df = manager.update_data(
#     start_year=2023,
#     start_month=1
# )