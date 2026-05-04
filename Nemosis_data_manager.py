import os
import shutil
import pandas as pd
import json
from datetime import datetime, timedelta
from nemosis import dynamic_data_compiler


class NEMDispatchLoadManager:
    """
    使用 nemosis 获取 AEMO 数据并存为 parquet，包含自动清理临时文件功能
    """

    def __init__(
            self,
            storage_dir=r"/Users/gzcheng/Desktop/Udacity/_history_data_for_investment",
            table_name="DISPATCHLOAD",
            raw_cache_dir=None
    ):
        self.api_name = "nemosis"
        self.table_name = table_name
        self.interval_str = "5min"

        # 临时缓存目录
        if not raw_cache_dir:
            self.raw_cache_dir = os.path.join(storage_dir, self.interval_str, "nemo_cache")
        else:
            self.raw_cache_dir = raw_cache_dir
        os.makedirs(self.raw_cache_dir, exist_ok=True)

        # 最终 Parquet 存储目录
        self.storage_dir = os.path.join(storage_dir, self.interval_str)
        os.makedirs(self.storage_dir, exist_ok=True)

        self.metadata_path = os.path.join(
            self.storage_dir,
            f"metadata_{self.api_name}_{self.table_name}_{self.interval_str}.json"
        )
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_metadata(self, final_df):
        payload = {
            "api": self.api_name,
            "table": self.table_name,
            "rows": int(len(final_df)),
            "start_utc": str(final_df.index[0]),
            "end_utc": str(final_df.index[-1]),
            "updated_at": datetime.utcnow().isoformat(),
            "file_path": self._get_file_path()
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

    def update_data(self, start_time="2024/01/01 00:00:00", end_time=None, cleanup=True):
        """
        更新数据并可选是否清理临时文件
        """
        file_path = self._get_file_path()

        # 1. 增量时间判定
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path)
            if not isinstance(existing_df.index, pd.DatetimeIndex):
                existing_df.index = pd.to_datetime(existing_df.index)
            last_ts = existing_df.index[-1]
            fetch_start = (last_ts + timedelta(minutes=5)).strftime("%Y/%m/%d %H:%M:%S")
        else:
            existing_df = pd.DataFrame()
            fetch_start = start_time

        if end_time is None:
            end_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        # 2. 调用 nemosis 下载
        print(f"正在从 AEMO 下载 {self.table_name}...")
        new_df = dynamic_data_compiler(
            start_time=fetch_start,
            end_time=end_time,
            table_name=self.table_name,
            raw_data_location=self.raw_cache_dir
        )

        if new_df is None or new_df.empty:
            print("未获取到新数据。")
            if cleanup: self._cleanup_temp_files()
            return existing_df

        # 3. 处理索引与合并
        if 'SETTLEMENTDATE' in new_df.columns:
            new_df['SETTLEMENTDATE'] = pd.to_datetime(new_df['SETTLEMENTDATE'])
            new_df.set_index('SETTLEMENTDATE', inplace=True)
        new_df = new_df.sort_index()

        if not existing_df.empty:
            final_df = pd.concat([existing_df, new_df])
            final_df = final_df[~final_df.index.duplicated(keep='last')].sort_index()
        else:
            final_df = new_df

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