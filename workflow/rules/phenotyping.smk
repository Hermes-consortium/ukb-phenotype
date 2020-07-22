# --------------
# Configuration
# --------------
import pandas as pd
from pathlib import Path

configfile: "workflow/config/phenotype.yaml"
report: "workflow/report/workflow.rst"

# Allow users to fix the underlying OS via singularity.
singularity: "docker://continuumio/miniconda3"

# --------------
# Target file(s)
# --------------
def all_pheno():
    p = Path("data") / "code_list"
    files = list(p.glob("*.tsv"))
    target_dir = Path("results") / ("app" + config["target_app"])
    target_files = [target_dir / f.stem / "all.txt" for f in files]
    return [f.as_posix() for f in target_files]

rule all_pheno:
    input:
        *all_pheno()


# --------------
# Rules
# --------------

rule get_eid_case:
    input:
        code_list="data/code_list/{pheno}.tsv",
        data="data/app{app_id}/{dataset}.txt"
    output:
        "results/app{app_id}/{pheno}/{dataset}.txt"
    run:
        header = pd.read_table(input.data, nrows=1).columns
        df_code = pd.read_table(input.code_list)

        if wildcards.dataset == "hesin_diag":
            query = 'Source == "ICD10" | Source == "ICD9"'
            cols = ["eid", "diag_icd9", "diag_icd9_nb", "diag_icd10", "diag_icd10_nb"]
        elif wildcards.dataset == "hesin_oper":
            query = 'Source == "OPCS4"'
            cols = ["eid", "oper4"]

        codes = df_code.query(query).Code

        if codes.empty:
            raise ValueError

        codes = codes.str.replace("[.*]", "")
        codes = [c + ".*" if len(c) < 4 else c for c in codes]

        regex = "(" + "|".join(codes) + ")"
        cols_ix = [str(header.get_loc(c) + 1) for c in cols if c in header]
        cols_ix_merged = ",".join(cols_ix)

        shell("grep -wE '{regex}' <(cut -f{cols_ix_merged} {input.data}) \
               | cut -f{cols_ix[0]} | sort -u > {output}")

def expect_data(wc):
    df_code = pd.read_table(Path("data") / "code_list" / (wc.pheno + ".tsv"))
    dict_dataset = dict(ICD9 = 'hesin_diag', ICD10 = 'hesin_diag', OPCS4 = 'hesin_oper')
    dataset = df_code.query('Source in @dict_dataset')\
                .Source.replace(dict_dataset).unique().tolist()
    files = [Path("results") / ("app" + wc.app_id) / wc.pheno / (d + ".txt") for d in dataset]
    return [f.as_posix() for f in files]

rule concat_data:
    input:
        expect_data
    output:
        "results/app{app_id}/{pheno}/all.txt"
    shell:
        "cat {input} | sort -u > {output}"
