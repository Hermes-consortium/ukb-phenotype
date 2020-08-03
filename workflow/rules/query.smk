# --------------
# Configuration
# --------------
import pandas as pd
from pathlib import Path
import yaml
report: "workflow/report/workflow.rst"

# Allow users to fix the underlying OS via singularity.
singularity: "docker://continuumio/miniconda3"

ruleorder: move_data_chunk > join_data

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
        temp(directory("results/{project}"))
    run:
        with open(str(input)) as f:
            query = yaml.full_load(f)
        dir_out = Path(str(output))
        dir_out.mkdir(parents = True, exist_ok = True)

        for i in range(len(query['data'])):
            outfile = dir_out / (str(i + 1) + '.yaml')
            with open(outfile, 'w') as o:
                yaml.dump(query['data'][i], o, default_flow_style=False)

checkpoint check_yaml:
    input:
        "results/{project}/{chunk}.yaml"
    output:
        temp("results/{project}/{chunk}.checked.yaml")
    shell:
        "cp {input} {output}"

def parse_yaml_ukb(wc):
    query_file = Path("results") / wc.project / (str(wc.chunk) + ".checked.yaml")
    with open(query_file) as f:
        query = yaml.full_load(f)
    db = Path("data") / ("app" + str(query['app'])) / (query['dataset'] + ".db")
    df_profile = Path("data") / ("app" + str(query['app'])) / (query['dataset'] + ".profile.tsv")
    return {k: v.as_posix() for k,v in dict(yaml = query_file, db = db, profile = df_profile).items()}

rule fetch_ukb:
    input:
        unpack(parse_yaml_ukb)
    output:
        temp("results/{project}/{chunk}.ukb.tsv")
    script:
        "scripts/fetch_ukb.R"

def parse_yaml_ehr(wc):
    query_file = Path("results") / wc.project / (str(wc.chunk) + ".checked.yaml")
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
                q_pheno = q_pheno + [i for i in v if i not in phenos]

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
                        path = (Path("data") / ("app" + str(query['app'])) / "ehr_case" / k / (d + ".txt")).as_posix()
                        phenos = phenos + [k]
                        case_list.append(path)
        else:
            path = (Path("data") / ("app" + str(query['app'])) / "ehr_case" / p / "all.txt").as_posix()
            phenos.append(p)
            case_list.append(path)

    return dict(yaml = query_file.as_posix(), case_list = case_list)

rule fetch_ehr:
    input:
        unpack(parse_yaml_ehr)
    output:
        temp("results/{project}/{chunk}.ehr.tsv")
    params:
        case_code = 1,
        noncase_code = 0
    script:
        "scripts/fetch_ehr.R"


def check_data_type(wc):
    with checkpoints.check_yaml.get(**wc).output[0].open() as f:
        query = yaml.full_load(f)
        if query['dataset'] == "ehr":
            return "results/{project}/{chunk}.ehr.tsv"
        else:
            return "results/{project}/{chunk}.ukb.tsv"

rule move_data_chunk:
    input:
        check_data_type
    output:
        temp("results/{project}/{chunk}.tsv")
    shell:
        "mv {input} {output}"

def collect_tsv(wc):
    dir = checkpoints.split_yaml.get(**wc).output[0]
    query_file = Path("data/query") / (wc.project + ".yaml")
    with open(query_file, "r") as f:
        query = yaml.full_load(f)

    sample_file = [d['sample_file'] for d in query['data']]
    data = expand("results/{project}/{chunk}.tsv",
                  project = wc.project,
                  chunk = glob_wildcards(Path(dir)/"{chunk}.yaml").chunk)
    return dict(sample = sample_file, data = data)

rule join_data:
    input:
        unpack(collect_tsv)
    output:
        "results/{project}.tsv"
    script:
        "scripts/join_data.R"
