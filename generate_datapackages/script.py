import pandas as pd
import requests

dataset_ids = ["3300", "2292"]


def download_data(dataset_id):
    url = f"https://www.bco-dmo.org/dataset/{dataset_id}/data/download"
    return pd.read_csv(url, sep="\t", comment="#")


def get_latlon_fields(dataset_id):
    url = f"https://lod.bco-dmo.org/sparql?query=PREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0D%0APREFIX+rdfs%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0APREFIX+arpfo%3A+%3Chttp%3A%2F%2Fvocab.ox.ac.uk%2Fprojectfunding%23%3E%0D%0APREFIX+dc%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Felements%2F1.1%2F%3E%0D%0APREFIX+dcat%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E%0D%0APREFIX+dcterms%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0D%0APREFIX+foaf%3A+%3Chttp%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2F%3E%0D%0APREFIX+geo%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2003%2F01%2Fgeo%2Fwgs84_pos%23%3E%0D%0APREFIX+geosparql%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fgeosparql%23%3E%0D%0APREFIX+odo%3A+%3Chttp%3A%2F%2Focean-data.org%2Fschema%2F%3E%0D%0APREFIX+participation%3A+%3Chttp%3A%2F%2Fpurl.org%2Fvocab%2Fparticipation%2Fschema%23%3E%0D%0APREFIX+prov%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fprov%23%3E%0D%0APREFIX+rs%3A+%3Chttp%3A%2F%2Fjena.hpl.hp.com%2F2003%2F03%2Fresult-set%23%3E%0D%0APREFIX+schema%3A+%3Chttp%3A%2F%2Fschema.org%2F%3E%0D%0APREFIX+sd%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fsparql-service-description%23%3E%0D%0APREFIX+sf%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fsf%23%3E%0D%0APREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+time%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2006%2Ftime%23%3E%0D%0APREFIX+void%3A+%3Chttp%3A%2F%2Frdfs.org%2Fns%2Fvoid%23%3E%0D%0APREFIX+xsd%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23%3E%0D%0ASELECT+%3Fdataset+%3Flon_column+%3Flon+%3Flat_column+%3Flat%0D%0AWHERE+%7B%0D%0A++%3Flat+%3Flinked+%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP09%2Fcurrent%2FLATX%2F%3E+.%0D%0A++%3Flon+%3Flinked+%3Chttp%3A%2F%2Fvocab.nerc.ac.uk%2Fcollection%2FP09%2Fcurrent%2FLONX%2F%3E+.%0D%0A++%3Fdplon+odo%3AisInstanceOf+%3Flon+.%0D%0A++%3Fdplon+skos%3AprefLabel+%3Flon_column+.%0D%0A++%3Fdataset+odo%3AstoresValuesFor+%3Fdplon+.%0D%0A++%3Fdplat+odo%3AisInstanceOf+%3Flat+.%0D%0A++%3Fdataset+odo%3AstoresValuesFor+%3Fdplat+.%0D%0A++%3Fdplat+skos%3AprefLabel+%3Flat_column+.%0D%0A++%3Fdataset+a+odo%3ADataset+.%0D%0A++%3Fdataset+odo%3Aidentifier+%3Fdataset_id+.%0D%0A++%3Fdataset_id+a+odo%3ABCODMOIdentifier+.%0D%0A++%3Fdataset_id+odo%3AresolvableURL+%3Fdataset_id_url+.%0D%0A++%3Fdataset_id+odo%3AidentifierValue+%3Fdataset_id_value+.%0D%0A++FILTER+%28STR%28%3Fdataset_id_value%29+%3D+%22{dataset_id}%22%29%0D%0A++FILTER+%28STR%28%3Fdataset_id_url%29+%3D+STR%28%3Fdataset%29%29%0D%0A%7D%0D%0AORDER+BY+%3Fdataset+&output=json"
    r = requests.get(url)
    js = r.json()
    res = js.get("results", {}).get("bindings", {})

    assert len(res) in [0, 1]

    if len(res) == 0:
        return None, None

    lat_column = res[0].get("lat_column", {}).get("value", None)
    lon_column = res[0].get("lon_column", {}).get("value", None)

    return lat_column, lon_column


def get_species_fields(dataset_id):
    url = f"https://lod.bco-dmo.org/sparql?query=PREFIX+rdf%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0D%0APREFIX+rdfs%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0D%0APREFIX+owl%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2002%2F07%2Fowl%23%3E%0D%0APREFIX+arpfo%3A+%3Chttp%3A%2F%2Fvocab.ox.ac.uk%2Fprojectfunding%23%3E%0D%0APREFIX+dc%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Felements%2F1.1%2F%3E%0D%0APREFIX+dcat%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E%0D%0APREFIX+dcterms%3A+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0D%0APREFIX+foaf%3A+%3Chttp%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2F%3E%0D%0APREFIX+geo%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2003%2F01%2Fgeo%2Fwgs84_pos%23%3E%0D%0APREFIX+geosparql%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fgeosparql%23%3E%0D%0APREFIX+odo%3A+%3Chttp%3A%2F%2Focean-data.org%2Fschema%2F%3E%0D%0APREFIX+participation%3A+%3Chttp%3A%2F%2Fpurl.org%2Fvocab%2Fparticipation%2Fschema%23%3E%0D%0APREFIX+prov%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fprov%23%3E%0D%0APREFIX+rs%3A+%3Chttp%3A%2F%2Fjena.hpl.hp.com%2F2003%2F03%2Fresult-set%23%3E%0D%0APREFIX+schema%3A+%3Chttp%3A%2F%2Fschema.org%2F%3E%0D%0APREFIX+sd%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fsparql-service-description%23%3E%0D%0APREFIX+sf%3A+%3Chttp%3A%2F%2Fwww.opengis.net%2Font%2Fsf%23%3E%0D%0APREFIX+skos%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2004%2F02%2Fskos%2Fcore%23%3E%0D%0APREFIX+time%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2006%2Ftime%23%3E%0D%0APREFIX+void%3A+%3Chttp%3A%2F%2Frdfs.org%2Fns%2Fvoid%23%3E%0D%0APREFIX+xsd%3A+%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23%3E%0D%0APREFIX+param%3A+%3Chttp%3A%2F%2Flod.bco-dmo.org%2Fid%2Fparameter%2F%3E%0D%0ASELECT+%3Fdataset+%3Fspecies_column+%3Fparameter%0D%0AWHERE+%7B%0D%0A++VALUES+%3Fparameter+%7B+param%3A976+param%3A994+param%3A1837+param%3A2010+param%3A1869+param%3A476054+param%3A995+param%3A2008+param%3A912+param%3A928+param%3A830+%7D%0D%0A++%3Fdpspecies+odo%3AisInstanceOf+%3Fparameter+.%0D%0A++%3Fdataset+odo%3AstoresValuesFor+%3Fdpspecies+.%0D%0A++%3Fdataset+a+odo%3ADataset+.%0D%0A++%3Fdpspecies+skos%3AprefLabel+%3Fspecies_column+.%0D%0A++%3Fdataset+odo%3Aidentifier+%3Fdataset_id+.%0D%0A++%3Fdataset_id+a+odo%3ABCODMOIdentifier+.%0D%0A++%3Fdataset_id+odo%3AresolvableURL+%3Fdataset_id_url+.%0D%0A++%3Fdataset_id+odo%3AidentifierValue+%3Fdataset_id_value+.%0D%0A++FILTER+%28STR%28%3Fdataset_id_value%29+%3D+%22{dataset_id}%22%29%0D%0A++FILTER+%28STR%28%3Fdataset_id_url%29+%3D+STR%28%3Fdataset%29%29%0D%0A%7D%0D%0AORDER+BY+%3Fdataset+&output=json"
    r = requests.get(url)
    js = r.json()
    res = js.get("results", {}).get("bindings", {})

    return_list = []
    for result in res:
        species_column = result.get("species_column", {}).get("value", None)
        return_list.append(species_column)

    return return_list


def get_unique_species(df, species):
    return_list = []
    for s in species:
        assert s in df
        return_list.append(list(df[s].unique()))

    return return_list


for dataset_id in dataset_ids:
    df = download_data(dataset_id)
    lat, lon = get_latlon_fields(dataset_id)
    species = get_species_fields(dataset_id)
    unique_species = get_unique_species(df, species)
    print(df)
    print(lat, lon)
    print(species)
    print(unique_species)
    print()
    print()
