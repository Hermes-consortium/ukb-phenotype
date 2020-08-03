# --------------
# Configuration
# --------------
import pandas as pd
from pathlib import Path
import yaml

report: "workflow/report/workflow.rst"

# Allow users to fix the underlying OS via singularity.
singularity: "docker://continuumio/miniconda3"

# --------------
# Target file(s)
# --------------

# --------------
# Rules
# --------------

checkpoint split_yaml:
    input:
        "data/query/{project}.yaml"
    output:
        directory("data/query/{project}/")
    run:
        query_file = Path("data/query") / (wildcards.project + ".yaml")
        with open(query_file) as f:
            query = yaml.full_load(f)

        # data = dict(
        #     ehr = [d for d in query['data'] if d["dataset"] == "ehr"],
        #     ukb = [d for d in query['data'] if d["dataset"].startswith("ukb")]
        # )

        dir_out = query_file.with_suffix('')
        dir_out.mkdir(parents = True, exist_ok = True)

        # for d in data:
        for i in range(len(query['data'])):
            outfile = dir_out / (str(i + 1) + '.yaml')
            with open(outfile, 'w') as o:
                yaml.dump(query['data'][i], o, default_flow_style=False)

def collect_yaml(wc):
    dir = checkpoints.split_yaml.get(**wc).output[0]
    return expand("data/query/{project}/{chunk}.yaml",
                  project = wc.project,
                  chunk = glob_wildcards((Path(dir) / "{chunk}.yaml").as_posix()).chunk)

rule collect_yaml:
    input:
        collect_yaml
    output:
        temp(touch("data/query/{project}.log"))


checkpoint check_yaml:
    input:
        "data/query/{project}/{chunk}.yaml"
    output:
        "data/query/{project}/{chunk}.type.yaml"
    shell:
        "awk '$1 ~ /dataset:/ {{print $2; exit}}' {input} > {output}"


rule process_ukb:
    input:
        "data/query/{project}/{chunk}.type.yaml"
    output:
        "data/query/{project}/{chunk}.ukb.tsv"
    shell:
        "echo 'ukb' > {output}"

rule process_ehr:
    input:
        "data/query/{project}/{chunk}.type.yaml"
    output:
        "data/query/{project}/{chunk}.ehr.tsv"
    shell:
        "echo 'ehr' > {output}"


def aggregate_input(wildcards):
    with checkpoints.check_yaml.get(**wildcards).output[0].open() as f:
        if f.read().strip() == "ehr":
            return "data/query/{project}/{chunk}.ehr.tsv"
        else:
            return "data/query/{project}/{chunk}.ukb.tsv"

rule aggregate:
    input:
        aggregate_input
    output:
        "data/query/{project}/{chunk}.tsv"
    shell:
        "cp {input} {output}"

# def collect_tsv(wc):
#     dir = checkpoints.split_yaml.get(**wc).output[0]
#     return expand("data/query/{project}/{chunk}.tsv",
#                   project = wc.project,
#                   chunk = glob_wildcards((Path(dir) / "{chunk}.tsv").as_posix()).chunk)

rule all_data:
    input:
        expand("data/query/{project}/{chunk}.tsv",
                allow_missing = True,
                chunk = glob_wildcards("data/query/{project}/{chunk}.tsv").chunk)
    output:
        "data/query/{project}.data.tsv"
    shell:
        "cat {input} > {output}"


# def collect_yaml(wc):
#     dir = checkpoints.split_yaml.get(**wc).output[0]
#     return expand("data/query/{project}/{chunk}.yaml",
#                   project = wc.project,
#                   chunk = glob_wildcards((dir / "{chunk}.yaml").as_posix()).chunk)
    # with checkpoints.split_yaml.get(**wc).output[0].open() as f:
    #     if f.read().strip().startswith("ehr"):
    #         return "data/query/{project}/ehr.{chunk}.yaml"
    #     else:
    #         return "data/query/{project}/ukb.{chunk}.yaml"

def parse_yaml_ukb(wc):
    query_file = Path("data/query") / wc.project / ("ukb." + str(wc.chunk) + ".yaml")
    with open(query_file) as f:
        query = yaml.full_load(f)
    db = Path("data") / ("app" + str(query['app'])) / (query['dataset'] + ".db")
    df_profile = Path("data") / ("app" + str(query['app'])) / (query['dataset'] + ".profile.tsv")
    return {k: v.as_posix() for k,v in dict(yaml = query_file, db = db, profile = df_profile).items()}

rule fetch_ukb:
    input:
        unpack(parse_yaml_ukb)
    output:
        "data/query/{project}/ukb.{chunk}.tsv"
    script:
        "scripts/fetch_ukb.R"


def parse_yaml_ehr(wc):
    query_file = Path("data/query") / wc.project / ("ehr." + str(wc.chunk) + ".yaml")
    with open(query_file) as f:
        query = yaml.full_load(f)
    # db = Path("data") / ("app" + str(query['app'])) / (query['dataset'] + ".db")

    if 'pheno' in query:
        q_pheno = query['pheno']
        phenos = [list(p.keys())[0] if type(p) is dict else p for p in q_pheno]
    else:
        q_pheno = []

    if 'composite_pheno' in query:
        for pheno in query['composite_pheno']:
            for k,v in pheno.items():
                if v not in phenos:
                    q_pheno + v

    if not q_pheno:
        raise ValueError

    dict_dataset = dict(ICD9 = ['hesin_diag'],
                        ICD10 = ['hesin_diag', 'death_cause'],
                        OPCS4 = ['hesin_oper'])

    case_list = []
    for p in q_pheno:
        if type(p) is dict:
            for k,v in p.items():
                df_code = pd.read_table(Path("data") / "code_list" / (k + ".tsv"))
                data_source = df_code.query('Source in @dict_dataset').Source.unique().tolist()
                dataset = [v for k,v in dict_dataset.items() if k in data_source]
                dataset = list(set([i for d in dataset for i in d]))

                for d in v:
                    if d in dataset:
                        path = (Path("results") / ("app" + str(query['app'])) / k / (d + ".txt")).as_posix()
                        phenos = phenos + [k]
                        case_list.append(path)
        else:
            path = (Path("results") / ("app" + str(query['app'])) / p / "all.txt").as_posix()
            phenos.append(p)
            case_list.append(path)

    return dict(yaml = query_file.as_posix(), case_list = case_list)

rule fetch_ehr:
    input:
        unpack(parse_yaml_ehr)
    output:
        "data/query/{project}/ehr.{chunk}.tsv"
    script:
        "scripts/fetch_ehr.R"


# def collect_query(wc):
#     dir_out = Path(checkpoints.split_yaml.get(**wc).output[0])
#     return expand("results/query/{project}/{chunk}.csv",
#                   project = wc.project,
#                   chunk = glob_wildcards((dir_out / "{chunk}.yaml").as_posix()).chunk
#                   )
#
#
# rule collect_query:
#     input:
#         collect_query
#     output:
#         "results/query/{project}.csv"



# def query_input(wc):
#     query_file = Path("data/query") / (wc.project + ".yaml")
#     with open(query_file) as f:
#         query = yaml.full_load(f)
#
#     df_query = pd.json_normalize(query['data'])
#     list_db = [(Path("data") / ("app" + str(a)) / (d + ".db")).as_posix()
#                for a,d in zip(df_query.app, df_query.dataset)]
#     list_profile = [(Path("data") / ("app" + str(a)) / (d + ".profile.tsv")).as_posix()
#                     for a,d in zip(df_query.app, df_query.dataset)
#                     if d.startswith('ukb')]
#     return dict(yaml = query_file, db = list_db, profile = list_profile)
#
#
# rule query_pheno:
#     input:
#         unpack(query_input)
#     output:
#         "results/query/{project}.{format}"
#     wildcard_constraints:
#         format="(csv|tsv)"
#     script:
#         "scripts/into_sqlite.R"
