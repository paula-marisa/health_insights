import os
from pathlib import Path

# Usar pyreaddbc para converter .dbc -> .dbf (se existir no ambiente)
try:
    from pyreaddbc.readdbc import dbc2dbf
    HAS_PYREADDBC = True
except Exception:
    HAS_PYREADDBC = False

from dbfread import DBF
import pandas as pd

INPUT_DIR = Path("datasets/originais")
OUTPUT_DIR = Path("datasets/csv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def convert_dbf_to_csv(dbf_path: Path, csv_path: Path):
    # tentar latin-1; se der pau, tentar iso-8859-1
    try:
        table = DBF(str(dbf_path), encoding="latin-1")
        df = pd.DataFrame(iter(table))
    except UnicodeDecodeError:
        table = DBF(str(dbf_path), encoding="iso-8859-1")
        df = pd.DataFrame(iter(table))
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"CSV gerado: {csv_path.name} ({len(df):,} linhas)")

def ensure_dbf_from_dbc(dbc_path: Path) -> Path:
    """Se existir .dbc e não existir o .dbf correspondente, converte usando pyreaddbc."""
    dbf_path = dbc_path.with_suffix(".dbf")
    if dbf_path.exists():
        return dbf_path
    if not HAS_PYREADDBC:
        raise RuntimeError(
            f"Não encontrei pyreaddbc para converter {dbc_path.name}. "
            f"No WSL, corre:  pip install pyreaddbc"
        )
    print(f"Convertendo DBC → DBF: {dbc_path.name} → {dbf_path.name}")
    dbc2dbf(str(dbc_path), str(dbf_path))
    if not dbf_path.exists() or dbf_path.stat().st_size == 0:
        raise RuntimeError(f"Falha ao gerar DBF a partir de {dbc_path.name}")
    return dbf_path

def main():
    # listar ficheiros
    dbf_files = sorted(p for p in INPUT_DIR.glob("*.dbf"))
    dbc_files = sorted(p for p in INPUT_DIR.glob("*.dbc"))

    if not dbf_files and not dbc_files:
        print(f"Nada para processar em {INPUT_DIR.resolve()}")
        return

    # garantir DBF para todos os DBC
    for dbc in dbc_files:
        try:
            ensure_dbf_from_dbc(dbc)
        except Exception as e:
            print(f"Erro a converter {dbc.name}: {e}")

    # 2) (re)carregar lista de DBFs após conversões
    dbf_files = sorted(p for p in INPUT_DIR.glob("*.dbf"))

    total = 0
    for dbf in dbf_files:
        csv_path = OUTPUT_DIR / (dbf.stem + ".csv")
        try:
            print(f"{dbf.name} → {csv_path.name}")
            convert_dbf_to_csv(dbf, csv_path)
            total += 1
        except Exception as e:
            print(f"Erro ao processar {dbf.name}: {e}")

    print(f"\nConcluído. {total} ficheiro(s) convertido(s) para CSV em {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
