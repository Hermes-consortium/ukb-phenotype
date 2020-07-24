# --------------
# Configuration
# --------------
import pandas as pd
from pathlib import Path
import subprocess

report: "workflow/report/workflow.rst"

# Allow users to fix the underlying OS via singularity.
singularity: "docker://continuumio/miniconda3"

# --------------
# Target file(s)
# --------------


# --------------
# Rules
# --------------

# Prepare data --------------------------------
rule check_md5:
    input:
        data_enc="data/app{app_id}/ukb{data_id}.enc",
        md5_email="data/app{app_id}/ukb{data_id}.md5"
    output:
        temp("data/app{app_id}/ukb{data_id}_md5_check.txt")
    run:
        with open(input.md5_email) as f:
            md5_email = f.read()
        proc = subprocess.Popen(["ukbmd5", input.data_enc], stdout=subprocess.PIPE)
        md5_dl = proc.communicate()[0].decode()
        if md5_email in md5_dl:
            print("MD5 check passed")
            with open(str(output), "w") as f:
                f.write("PASS")
        else:
            raise WorkflowError

rule ukb_unpack:
    input:
        status="data/app{app_id}/ukb{data_id}_md5_check.txt",
        data_enc="data/app{app_id}/ukb{data_id}.enc",
        key="data/app{app_id}/k{app_id}r{data_id}.key"
    output:
        "data/app{app_id}/ukb{data_id}.enc_ukb"
    shell:
        "ukbunpack {input.data_enc} {input.key}"

rule dl_encoding:
    output:
        "data/app{app_id}/encoding.ukb"
    shell:
        "wget -nd biobank.ctsu.ox.ac.uk/crystal/util/encoding.ukb"

# Create data dictionary and profile --------------------------------
rule get_data_dict:
    input:
        encoding="data/app{app_id}/encoding.ukb",
        data_enc_ukb="data/app{app_id}/ukb{data_id}.enc_ukb"
    output:
        "data/app{app_id}/ukb{data_id}.html"
    shell:
        "ukbconv {input.data_enc_ukb} docs -e {input.encoding}"

rule get_data_profile:
    input:
        "data/app{app_id}/ukb{data_id}.html"
    output:
        "data/app{app_id}/ukb{data_id}.profile.tsv"
    script:
        "scripts/parse_html.R"

rule get_field_by_type:
    input:
        "data/app{app_id}/ukb{data_id}.profile.tsv"
    output:
        expand("data/app{app_id}/by_type/ukb{data_id}_{type}_fields.txt",
               type = ["singleCat", "multiCat", "str", "int", "float"],
               allow_missing = True)
    run:
        df_profile = pd.read_table(str(input))
        for t in ["singleCat", "multiCat", "str", "int", "float"]:
            f = "ukb" + wildcards.data_id + "_" + t + "_fields.txt"
            path_f = Path("data") / ("app" + wildcards.app_id) / "by_type" / f
            with open(path_f, "w+") as o:
                UDI = df_profile.query('db == @t').UDI
                fields = UDI.replace(regex = r'-.*', value ='').unique()
                pd.Series(fields).to_csv(o, index=False, header=False)

# Convert encrypted data --------------
rule conv_data_all:
    input:
        encoding="data/app{app_id}/encoding.ukb",
        data_enc_ukb="data/app{app_id}/ukb{data_id}.enc_ukb"
    output:
        "data/app{app_id}/ukb{data_id}.{format}"
    wildcard_constraints:
        format="(csv|tsv)"
    run:
        if wildcards.format == "csv":
            opt = "csv"
        elif wildcards.format == "tsv":
            opt = "txt"
        else:
            print("format not supported")
            raise WorkflowError

        shell("ukbconv {input.data_enc_ukb} {opt} -e{input.encoding} -o{output}")


rule conv_data_subset:
    input:
        encoding="data/app{app_id}/encoding.ukb",
        data_enc_ukb="data/app{app_id}/ukb{data_id}.enc_ukb",
        fields="data/app{app_id}/by_type/ukb{data_id}_{type}_fields.txt"
    output:
        "data/app{app_id}/by_type/ukb{data_id}_{type}.{format}"
    wildcard_constraints:
        format="(csv|tsv)"
    run:
        if wildcards.format == "csv":
            opt = "csv"
        elif wildcards.format == "tsv":
            opt = "txt"
        else:
            print("format not supported")
            raise WorkflowError

        shell("ukbconv {input.data_enc_ukb} {opt} \
              -e{input.encoding} -i{input.fields} -o{output}")
        shell("mv {output}.{opt} {output}")

# Reformat data --------------------------------
rule pivot_longer:
    input:
        "data/app{app_id}/by_type/ukb{data_id}_{type}.{format}"
    output:
        "data/app{app_id}/by_type/ukb{data_id}_{type}_long.{format}"
    script:
        "scripts/wide_to_long.R"

# put into sqlite db --------------------------------
rule into_sqlite:
    input:
        expand("data/app{app_id}/by_type/ukb{data_id}_{type}_long.csv",
               type = ["singleCat", "multiCat", "str", "int", "float"],
               allow_missing = True)
    output:
        "data/app{app_id}/ukb{data_id}.sqlite"
    script:
        "scripts/into_sqlite.R"

# rule conv_data_subset:
#     input:
#         encoding="data/app{app_id}/encoding.ukb",
#         data_enc_ukb="data/app{app_id}/ukb{data_id}.enc_ukb",
#         fields="data/app{app_id}/by_type/ukb{data_id}_{type}_fields.txt"
#     output:
#         "data/app{app_id}/by_type/ukb{data_id}_{type}.{format}"
#     wildcard_constraints:
#         format="(csv|tsv)"
#     run:
#         if wildcards.format == "csv":
#             opt = "csv"
#         elif wildcards.format == "tsv":
#             opt = "txt"
#         else:
#             print("format not supported")
#             raise WorkflowError
#
#         shell("ukbconv {input.data_enc_ukb} {opt} \
#               -e{input.encoding} -i{input.fields} -o{output}")
#         shell("mv {output}.{opt} {output}")
