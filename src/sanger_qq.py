#!/usr/bin/env python

"""sanger_qq.py: Outputs the number of sanger studies and datasets, and associated publications. """

__author__ = "Mauricio Moldes"
__version__ = "0.1"
__maintainer__ = "Mauricio Moldes"
__email__ = "mauricio.moldes@crg.eu"
__status__ = "Developement"

import logging
import psycopg2
import yaml

''' VERIFIES THE CONNECTION TO PLSQL '''


def connection_plsql(cfg):
    conn_string = "host='" + str(cfg['plsql']['host']) + "' dbname='" + str(
        cfg['plsql']['dbname']) + "' user='" + str(
        cfg['plsql']['user']) + "' password='" + str(cfg['plsql']['password']) + "' port = '" + str(
        cfg['plsql']['port']) + "'"
    conn_plsql = psycopg2.connect(conn_string)
    return conn_plsql


def get_studies_by_dac(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(
        " select distinct dac.ega_stable_id as DAC, st.ega_stable_id as Study, st.released_date_at_src as study_released"
        " from dac_table dac"
        " inner join dac_dataset_table ddt on dac.\"id\" = ddt.dac_id "
        " inner join dataset_table dt on ddt.dataset_id = dt.\"id\" "
        " inner join study_dataset_table sdt on dt.\"id\" = sdt.dataset_id "
        " inner join study_table st on sdt.study_id = st.\"id\" "
        " where dac.ega_stable_id in ('EGAC00001000000','EGAC00001000063','EGAC00001000144','EGAC00001000205','EGAC00001000237','EGAC00001000282','EGAC00001000334') and st.released_date_at_src >= '2014-10-01' "
        " order by st.ega_stable_id asc")
    records = cursor.fetchall()
    return records


""" where dac.ega_stable_id in ('EGAC00001000000','EGAC00001000063','EGAC00001000144','EGAC00001000205','EGAC00001000237','EGAC00001000282','EGAC00001000334') and st.released_date_at_src >= '2014-10-01' """


def get_studies_publications(conn_plsql, study):
    cursor = conn_plsql.cursor()
    cursor.execute(" SELECT article.cited_by, article.article_id"
                   " FROM bibliography.article"
                   " INNER JOIN bibliography.ega_study_article ON ega_study_article.article_id = article.article_id"
                   " WHERE ega_study_article.ega_accession_id = '" + study + "'")
    records = cursor.fetchall()
    return records


def studies_dataset_dacs(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(" select dac.ega_stable_id as DAC, st.ega_stable_id as Study, dt.ega_stable_id as Dataset"
                   " from dac_table dac"
                   " inner join dac_dataset_table ddt on dac.id = ddt.dac_id"
                   " inner join dataset_table dt on ddt.dataset_id = dt.id"
                   " inner join study_dataset_table sdt on dt.id = sdt.dataset_id"
                   " inner join study_table st on sdt.study_id = st.id"
                   " where dac.ega_stable_id in ('EGAC00001000000','EGAC00001000063','EGAC00001000144','EGAC00001000205','EGAC00001000237','EGAC00001000282','EGAC00001000334') and st.released_date_at_src >= '2014-10-01' "
                   " order by st.ega_stable_id asc;")
    records = cursor.fetchall()
    return records


""" READ CONFIG FILE """


def read_config():
    with open("../config/config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg


def get_total_studies_ega(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(" select count (*)"
                   " from study_table st "
                   " where final_release_status = 'RELEASED' and disabled_flag = false ")
    total_studies = cursor.fetchone()
    return total_studies[0]


def get_total_datasets_ega(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(" select count (*) "
                   " from dataset_table dt "
                   " where final_release_status = 'RELEASED' and disabled_flag = false ")
    total_datasets = cursor.fetchone()
    return total_datasets[0]


def count_sanger_study_dataset_percentange_ega(conn_plsql, unique_sanger_studies, unique_sanger_datasets):
    total_studies_ega = get_total_studies_ega(conn_plsql)
    total_datasets_ega = get_total_datasets_ega(conn_plsql)
    percentage_sanger_studies = (unique_sanger_studies / total_studies_ega) * 100  # expected 8,7
    percentage_sanger_datasets = (unique_sanger_datasets / total_datasets_ega) * 100  # expect 7.6
    return percentage_sanger_studies, percentage_sanger_datasets


""" MAIN BIBLIOGRAPHY METHOD"""


def sanger_qq():
    # read config file
    global cfg
    cfg = read_config()
    conn_plsql = None
    number_of_publications = 0
    publications ={}
    try:
        conn_plsql = connection_plsql(cfg)
        if conn_plsql:
            res = studies_dataset_dacs(conn_plsql)
            studies = []
            datasets = []
            for result in res:
                studies.append(result[1])
                datasets.append(result[2])
            unique_sanger_studies = len(set(studies))
            unique_sanger_datasets = len(set(datasets))
            percentage_sanger_studies, percentage_sanger_datasets = count_sanger_study_dataset_percentange_ega(
                conn_plsql, unique_sanger_studies, unique_sanger_datasets)
            results = get_studies_by_dac(conn_plsql)
            for study in results:
                dac = study[0]
                source = study[1]
                res = get_studies_publications(conn_plsql, source)
                if res is None:
                    continue
                else:
                    number_of_publications = number_of_publications + len(res)
                    for result in res:
                        pub_id = result[1]
                        citations= result[0]
                        publications.update({pub_id: citations})
                print(dac, source, res)
        else:
            print("conn fail")
    except Exception as e:
        print(e)
    number_of_publications = len(publications)
    number_of_citations = 0
    for key, value in publications.items():
        number_of_citations= number_of_citations + value
    print(publications)

    print("Sanger has contributed to the EGA by sharing " + str(len(set(studies))) + " studies and " + str(
        len(set(datasets))) + " datasets "
                              "since October 2014. These have been used in " + str(number_of_publications) + " publications that have received " + str(number_of_citations) + ""
        " citations combined."
        " Further, the Sanger Institute contribution to the EGA accounts for " + str(
        percentage_sanger_studies) + " per cent of all studies and " + str(
        percentage_sanger_datasets) + " per cent of all datasets shared on EGA since 2015. This equates to over 2.2 million downloads of Sanger datasets from the EGA. On average 65 million requests are made to EMBL-EBI websites every day.")


def main():
    try:
        # configure logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s [in %(pathname)s:%(lineno)d]'
        logging.basicConfig(format=log_format)
        # execute main function
        sanger_qq()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
