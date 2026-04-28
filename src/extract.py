import requests
import json
import os
import zipfile
import tempfile
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class DataExtractor:
    def __init__(self):
        # Sources API (CO2)
        self.base_url = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets"
        self.api_sources = {
            'co2_usage': 'emission-co2-perimetre-usage',
            'co2_complet': 'emission-co2-perimetre-complet'
        }
        # Sources GTFS
        self.gtfs_sources = {
            "sncf_ter": {
                "url": "https://eu.ftp.opendatasoft.com/sncf/gtfs/export-ter-gtfs-last.zip",
                "pays": "FR"
            },
            "sncf_intercites": {
                "url": "https://eu.ftp.opendatasoft.com/sncf/gtfs/export-intercites-gtfs-last.zip",
                "pays": "FR"
            },
            "db_germany": {
                "url": "https://download.gtfs.de/germany/fv_free/latest.zip",
                "pays": "DE"
            },
            "db_germany_regional": {
                "url": "https://download.gtfs.de/germany/rv_free/latest.zip",
                "pays": "DE"
            },
            "sncb_belgium": {
                "url": "https://gtfs.irail.be/nmbs/gtfs/latest.zip",
                "pays": "BE"
            }
        }
        self.proxies = None
        if os.getenv('HTTP_PROXY'):
            self.proxies = {
                'http': os.getenv('HTTP_PROXY'),
                'https': os.getenv('HTTPS_PROXY', os.getenv('HTTP_PROXY'))
            }

    # ----- API -----
    def extract_api_dataset(self, dataset_name, dataset_id):
        endpoint = f"{self.base_url}/{dataset_id}/records"
        all_records = []
        offset = 0
        limit = 100
        while True:
            params = {"limit": limit, "offset": offset}
            try:
                r = requests.get(endpoint, params=params, proxies=self.proxies, timeout=60)
                r.raise_for_status()
                data = r.json()
                records = data.get('results', [])
                all_records.extend(records)
                total = data.get('total_count', 0)
                print(f"    {len(all_records)}/{total}")
                if len(all_records) >= total:
                    break
                offset += limit
            except Exception as e:
                print(f"    Erreur API {dataset_name}: {e}")
                return None
        return all_records

    def save_json(self, data, name):
        os.makedirs('../data/raw', exist_ok=True)
        path = f"../data/raw/{name}_{datetime.now().strftime('%Y%m%d')}.json"
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"    Sauvegardé : {path}")
        return path

    # ----- GTFS -----
    def extract_gtfs_source(self, source_name, source_info):
        url = source_info["url"]
        try:
            r = requests.get(url, proxies=self.proxies, timeout=120)
            r.raise_for_status()
        except Exception as e:
            print(f"    Téléchargement GTFS {source_name} échoué : {e}")
            return False
        target_dir = f"../data/raw/gtfs/{source_name}"
        os.makedirs(target_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp.write(r.content)
            tmp_path = tmp.name
        try:
            with zipfile.ZipFile(tmp_path, 'r') as zf:
                zf.extractall(target_dir)
            print(f"    GTFS {source_name} extrait dans {target_dir}")
            return True
        except Exception as e:
            print(f"    Extraction GTFS {source_name} échouée : {e}")
            return False
        finally:
            os.unlink(tmp_path)

    def run_extraction(self):
        print(" EXTRACTION DES DONNÉES (API + GTFS)")
        # API
        for name, ds_id in self.api_sources.items():
            print(f"\n API {name}")
            records = self.extract_api_dataset(name, ds_id)
            if records:
                self.save_json(records, name)
        # GTFS
        print("\n TÉLÉCHARGEMENT DES SOURCES GTFS")
        for name, info in self.gtfs_sources.items():
            print(f"\n GTFS {name}")
            self.extract_gtfs_source(name, info)
        print("\n Extraction terminée")
        return True

if __name__ == "__main__":
    DataExtractor().run_extraction()