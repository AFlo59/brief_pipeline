import pandas as pd
from pathlib import Path
from typing import List, Optional
import datetime 
import requests
from requests.exceptions import RequestException

class NYCTaxiDataDownloader():

    def __init__(
            self,
            year: Optional[int] = None,
            data_dir: Path | str = "data/raw",
            base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data",
            timeout: int = 30,
        ) -> None:
            """
            - Définit BASE_URL, YEAR, DATA_DIR
            - Crée le répertoire si nécessaire
            """
            self.BASE_URL = base_url.rstrip("/")
            self.YEAR = int(year if year is not None else datetime.now().year)
            self.DATA_DIR = Path(data_dir)
            self.DATA_DIR.mkdir(parents=True, exist_ok=True)
            self._timeout = int(timeout)
    
    def get_file_path(self, month: int) -> Path:
          mm = f"{int(month):02d}"
          file_name = f"yellow_tripdata_{self.YEAR}-{mm}.parquet"
          return self.DATA_DIR / file_name
    
    def file_exists(self, month: int) -> bool:
          return self.get_file_path(month).is_file()
    
    def download_month(self, month: int) -> bool:
          path = self.get_file_path(month)
          if path.exists():
                print(f"skip {path.name} already exists.")
                return True
          mm = f"{int(month):02d}"
          url = f"{self.BASE_URL}/yello_tripdata_{self.YEAR}-{mm}.parquet"
          tmp_path = path.with_suffix(path.suffix + ".part")

          try:
            with requests.get(url, stream=True, timeout = self._timeout) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-lenght", "0"))
                downloaded = 0
                chunk_size = 8000
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size = chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            bar_len