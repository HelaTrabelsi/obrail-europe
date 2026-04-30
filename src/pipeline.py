import sys
import os
from datetime import datetime
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader


class ETLPipeline:
    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.start_time = None

    def log_step(self, step_name, success=True):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        status = "ok" if success else "erreur"
        print(f"\n{'='*60}")
        print(f"{status} {ts} — {step_name}")
        print(f"{'='*60}")

    def run_extract(self):
        print("\nÉTAPE 1 : EXTRACTION DES DONNÉES")
        try:
            success = self.extractor.run_extraction()
            self.log_step("Extraction", success)
            return success
        except Exception as e:
            print(f"Erreur extraction : {e}")
            self.log_step("Extraction", False)
            return False

    def run_transform(self):
        print("\n ÉTAPE 2 : TRANSFORMATION DES DONNÉES")
        try:
            data = self.transformer.run_transformation()
            success = data is not None
            self.log_step("Transformation", success)
            return success
        except Exception as e:
            print(f"Erreur transformation : {e}")
            self.log_step("Transformation", False)
            return False

    def run_load(self, clean_first=True):
        print("\n ÉTAPE 3 : CHARGEMENT EN BASE DE DONNÉES")
        try:
            success = self.loader.load_all_data(clean_first=clean_first)
            self.log_step("Chargement", success)
            return success
        except Exception as e:
            print(f"Erreur chargement : {e}")
            self.log_step("Chargement", False)
            return False

    def run_full_pipeline(self, clean_first=True):
        print("\n" + "=" * 60)
        print(" DÉMARRAGE DU PIPELINE ETL OBRAIL EUROPE")
        print(f" Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        self.start_time = datetime.now()

        for step in [
            self.run_extract,
            self.run_transform,
            lambda: self.run_load(clean_first=clean_first)
        ]:
            if not step():
                print("\n PIPELINE INTERROMPU — Une étape a échoué")
                return False

        duration = datetime.now() - self.start_time
        print("\n" + "=" * 60)
        print(" PIPELINE ETL TERMINÉ AVEC SUCCÈS")
        print(f"  Durée totale : {duration.total_seconds():.2f} secondes")
        print("=" * 60)
        return True


def main():
    parser = argparse.ArgumentParser(description='Pipeline ETL — ObRail Europe')
    parser.add_argument('--step', choices=['extract', 'transform', 'load', 'all'], default='all',
                        help="Étape à exécuter (défaut : all)")
    parser.add_argument('--no-clean', action='store_true',
                        help="Ne pas vider la base avant rechargement")
    args = parser.parse_args()

    pipeline = ETLPipeline()

    if args.step == 'extract':
        pipeline.run_extract()
    elif args.step == 'transform':
        pipeline.run_transform()
    elif args.step == 'load':
        pipeline.run_load(clean_first=not args.no_clean)
    else:
        pipeline.run_full_pipeline(clean_first=not args.no_clean)


if __name__ == "__main__":
    main()