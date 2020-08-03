# --------------
# Configuration
# --------------
import pandas as pd
from pathlib import Path
import yaml

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
        "data/app{app_id}/ehr_case/{pheno}/{dataset}.txt"
    run:
        header = pd.read_table(input.data, nrows=1).columns
        df_code = pd.read_table(input.code_list)

        if wildcards.dataset == "hesin_diag":
            query = 'Source == "ICD10" | Source == "ICD9"'
            cols = ["eid", "diag_icd9", "diag_icd9_nb", "diag_icd10", "diag_icd10_nb"]
        elif wildcards.dataset == "hesin_oper":
            query = 'Source == "OPCS4"'
            cols = ["eid", "oper4"]
        elif wildcards.dataset == "death_cause":
            query = 'Source == "ICD10"'
            cols = ["eid", "cause_icd10"]

        codes = df_code.query(query).Code

        if codes.empty:
            raise ValueError

        codes = codes.str.replace("[.*]", "")
        codes = [c + ".{1,2}" if len(c) < 4 else c for c in codes]

        regex = "(" + "|".join(codes) + ")"
        cols_ix = [str(header.get_loc(c) + 1) for c in cols if c in header]
        cols_ix_merged = ",".join(cols_ix)

        shell("grep -wE '{regex}' <(cut -f{cols_ix_merged} {input.data}) \
               | cut -f{cols_ix[0]} | sort -u > {output}")


def expect_data(wc):
    df_code = pd.read_table(Path("data") / "code_list" / (wc.pheno + ".tsv"))
    dict_dataset = dict(ICD9 = ['hesin_diag'],
                        ICD10 = ['hesin_diag', 'death_cause'],
                        OPCS4 = ['hesin_oper'])
    data_source = df_code.query('Source in @dict_dataset').Source.unique().tolist()

    dataset = [v for k, v in dict_dataset.items() if k in data_source]
    dataset = [i for d in dataset for i in d]
    dataset = list(set(dataset))

    files = [Path("data") / ("app" + wc.app_id) / "ehr_case" / wc.pheno / (d + ".txt")\
                for d in dataset if d in config["data_source"]]
    return [f.as_posix() for f in files]

rule concat_data:
    input:
        expect_data
    output:
        "data/app{app_id}/ehr_case/{pheno}/all.txt"
    run:
        # path = Path("results") / ("app" + wildcards.app_id) / wildcards.pheno
        # file_avail = [f.stem for f in list(path.glob("*.txt"))]
        # files = [path / (d + ".txt") for d in params.data if d in file_avail]
        # file_str = [f.as_posix() for f in files]

        shell("cat {input} | sort -u > {output}")

def get_composite_rule(wc):
    path = Path("data") / "composite_pheno_rule" / (wc.pheno + ".yaml")
    with open(path, "r") as f:
        phenos = yaml.full_load(f)
    files = [Path("data") / ("app" + wc.app_id) / "ehr_case" / p / "all.txt" for p in phenos]
    return [f.as_posix() for f in files]

rule combine_pheno:
    input:
        get_composite_rule
    output:
        "data/app{app_id}/ehr_case/composite_pheno/{pheno}.txt"
    shell:
        "cat {input} | sort -u > {output}"
