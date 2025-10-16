import pandas as pd
from pathlib import Path
from typing import List, Optional, Union
from datetime import datetime
import requests
from requests.exceptions import RequestException

class NYCTaxiDataDownloader():

    def __init__(
            self,
            year: Optional[int] = None,
            data_dir: Union[Path, str] = "data/raw",
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
        url = f"{self.BASE_URL}/yellow_tripdata_{self.YEAR}-{mm}.parquet"
        tmp_path = path.with_suffix(path.suffix + ".part")

        try:
            with requests.get(url, stream=True, timeout = self._timeout) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-lenght", "0"))
                downloaded = 0
                chunk_size = 8192
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size = chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            bar_len = 30
                            fill = (pct * bar_len) // 100
                            bar = "#" * fill + "-" * (bar_len - fill)
                            print(f"{bar} {pct:3d}% {downloaded}")
                if total > 0:
                    print()
            tmp_path.replace(path)
            print(f"{path.name}")
            return True

        except RequestException as e:
            print(f"requête échouée pour {url}: {e}")
        except Exception as e:
            print(f"Erreur innatendue pour l'{url}: {e}")
        finally:
            if tmp_path.exists() and not path.exists():
                try:
                    tmp_path.unlink()
                    print(f"Suppression du fichier partiel {tmp_path.name}")
                except Exception:
                    pass
        return False
    
    def download_all_available(self) -> list[Path]:
        now = datetime.now()
        if self.YEAR < now.year:
            last_month = 12
        elif self.YEAR == now.year:
            last_month = now.month
        else:
            print(f"year = {self.YEAR} est dans le futur, aucun téléchargement possible")
            return []
        present_files: List[Path] = []
        created = 0
        skipped = 0
        failed = 0
        print(f"Téléchargement {self.YEAR} - 01 -> {self.YEAR} - {last_month:02d} vers {self.DATA_DIR}")
        for m in range(1, last_month + 1):
            already = self.file_exists(m)
            ok = self.download_month(m)
            if ok:
                present_files.append(self.get_file_path(m))
                if already:
                    skipped += 1
                else:
                    created += 1
            else:
                failed += 1
        
        return present_files

if __name__ == "__main__":
    downloader = NYCTaxiDataDownloader(year=datetime.now().year)
    downloader.download_all_available()