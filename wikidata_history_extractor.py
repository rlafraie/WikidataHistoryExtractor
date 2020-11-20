# MIT License
#
# Copyright (c) 2020 Rashid Lafraie
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import sys
import os
from bs4 import BeautifulSoup
import re
from pathlib import Path
import hashlib
from urllib.request import urlopen
import bz2
from html import unescape
import json
from datetime import datetime
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import operator


def download_file(url, file):
    # Helper function to download large files in chunks #

    response = urlopen(url)
    chunk_size = 16 * 1024
    with open(file, 'wb') as f:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)


def get_current_timestamp():
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')


def process_checksum_file(checksum_file):
    # process checksums for xml dumps and write them into a separate file

    fld_of_checksum_file = checksum_file.parents[0]
    xml_dump_file_pattern = re.compile(r"[\s\S]*pages-meta-history.*\.bz2$$")

    with checksum_file.open() as file:
        for line in file:
            hash_, filename = line.split()
            if xml_dump_file_pattern.match(filename):
                checksum_filename = filename + "_checksum.txt"
                with open(fld_of_checksum_file / checksum_filename, mode="at", encoding="UTF-8") as out:
                    out.write(hash_)


def get_wikidata_dumps_urls(wikidata_dump_date):
    # Get list of xml dumps of the Wikidata history dumped at wikidata_dump_date and their corresponding URLs.

    wikidata_url = 'https://dumps.wikimedia.org'

    response = urlopen(wikidata_url + '/wikidatawiki/' + str(wikidata_dump_date))
    soup = BeautifulSoup(response, "html.parser")

    fld_of_checksum_file = Path.cwd() / "extraction_process_data" / "checksums_{}".format(wikidata_dump_date)
    fld_of_checksum_file.mkdir(exist_ok=True)

    checksum_output_file = fld_of_checksum_file / 'wikidatawiki-{}-md5sums.txt'.format(wikidata_dump_date)
    if not checksum_output_file.exists():
        checksum_el = soup.find('a', href=re.compile(r'[\s\S]*md5sums*\.txt$$'))
        checksum_file_url = wikidata_url + checksum_el.get('href')
        download_file(checksum_file_url, checksum_output_file)
        process_checksum_file(checksum_output_file)

    wikidata_history_xml_dumps = soup.find_all('a', href=re.compile(r'[\s\S]*pages-meta-history.*\.bz2$$'))
    url_per_dump_filename = [{"filename": element.getText(),
                              "url": element.get('href'),
                              "dumpdate": wikidata_dump_date}
                             for element in wikidata_history_xml_dumps]

    return url_per_dump_filename


def validate_file_checksum(file, wikidata_dump_date):
    # Compare checksums of file and listed checksum

    filename = file.name
    associated_checksum_filename = filename + "_checksum.txt"
    associated_checksum_file = Path.cwd() / "checksums_{}".format(wikidata_dump_date) / associated_checksum_filename
    with open(associated_checksum_file, mode="rt", encoding="UTF-8") as f:
        checksum = f.read()

    has_valid_checksum = checksum == hashlib.md5(open(file, 'rb').read()).hexdigest()
    if has_valid_checksum:
        print('File {} downloaded successfully'.format(filename))
    else:
        sys.exit('Downloaded File {} has wrong md5-hash!'.format(filename))


def download_xml_dump(file_download_dict, output_folder):
    # Downloads single xml dump of wikidata history and validates its checksum

    xml_dump_filename = file_download_dict["filename"]
    wikidata_hisory_dump_date = file_download_dict["dumpdate"]
    uri = file_download_dict["url"]

    xml_dump_output_file = output_folder / xml_dump_filename
    download_file('https://dumps.wikimedia.org/' + uri, xml_dump_output_file)
    validate_file_checksum(xml_dump_output_file, wikidata_hisory_dump_date)


def download_wikidata_history_dumps(wikidata_dump_date):
    # Download all xml dumps of the wikidata history dumped at <wikidata_dump_date>

    xml_dumps_output_fld = Path.cwd() / "extraction_process_data" / "xml_dumps_{}".format(wikidata_dump_date)
    downloaded_marker_fld = xml_dumps_output_fld / "downloaded_dumps_markers"
    downloaded_marker_fld.mkdir(parents=True, exist_ok=True)

    urls_of_xml_dumps_dict = get_wikidata_dumps_urls(wikidata_dump_date)
    for xml_dump_info in urls_of_xml_dumps_dict:
        xml_dump_filename = xml_dump_info["filename"]
        xml_dump_file = xml_dumps_output_fld / xml_dump_filename
        downloaded_marker_file = downloaded_marker_fld / "{}.downloaded".format(xml_dump_filename)

        if downloaded_marker_file.exists():
            print("Dump {} already downloaded - skipped.".format(xml_dump_filename))
            continue
        else:
            if xml_dump_file.exists():
                print("Dump {} exists but was previously aborted.".format(xml_dump_filename))
                xml_dump_file.unlink()
                print("Restart download of file {} at {}.".format(xml_dump_filename, get_current_timestamp()))
            else:
                print("Download file {} at {}.".format(xml_dump_filename, get_current_timestamp()))

            download_xml_dump(xml_dump_info, xml_dumps_output_fld)
            downloaded_marker_file.touch()


def create_item_revision_dict(item_id, revision_id, timestamp, claim_triple_list):
    # Create a dict holding information about item <item_id>

    item_revision_dict = {
        "item_id": item_id,
        "revision": revision_id,
        "timestamp": timestamp,
        "claims": claim_triple_list
    }

    return item_revision_dict


def create_log_entry_for_redirect_item(filename, source_item_id, target_item_id):
    redirects_log_fld = Path.cwd() / "extraction_process_data" / 'redirects'
    redirects_log_fld.mkdir(exist_ok=True)
    redirects_log_file = "{}_redirected_items.txt.bz2".format(filename)

    with bz2.open(redirects_log_file, mode="at", encoding="UTF-8") as f:
        f.write("{} {}\n".format(source_item_id[1:], target_item_id[1:]))


def get_item_redirects_dict():
    # Load entries in log file of redirected items into a dict

    redirects_log_folder = Path.cwd() / "extraction_process_data" / 'redirects'
    target_id_per_redirected_item = {}

    print("Load redirects from {} at {}.".format(redirects_log_folder, get_current_timestamp()))
    if redirects_log_folder.exists():
        for redirect_file_log in redirects_log_folder.iterdir():
            with bz2.open(redirect_file_log, "rt", encoding="UTF-8") as f:
                for line in f:
                    source_item_id, target_item_id = line.split()
                    target_id_per_redirected_item[source_item_id] = target_item_id

    print("Finished loading redirects at {}.".format(get_current_timestamp()))
    print("Counted {} redirects.".format(len(target_id_per_redirected_item)))
    return target_id_per_redirected_item


def extract_item_triple_operations(new_claims_set, old_claims_set, rev_ts, operation_type="ins"):
    # Synthesize triple operations between the claim lists of two subsequent revisions for a given item.

    item_triple_operations_list = []

    triple_operations = new_claims_set - old_claims_set if operation_type == "ins" else old_claims_set - new_claims_set
    for operation in triple_operations:
        subject_ = operation[0]
        predicate_ = operation[1]
        object_ = operation[2]
        operation_type = "+" if operation_type == "ins" else "-"
        item_triple_operations_list.append([subject_, predicate_, object_, operation_type, rev_ts])

    return item_triple_operations_list


def get_triple_operations_list(revision_file):
    # Determine all triple operations for a given item.

    item_triple_operations = []
    old_claims_set = set()
    new_claims_set = set()

    with bz2.open(revision_file, "rt", encoding="UTF-8") as jsonf:
        for line in jsonf:
            revision_dict = json.loads(line)
            item_id = revision_dict["item_id"]
            revision_ts = revision_dict["timestamp"]
            revision_claim_list = revision_dict["claims"]

            # Process claims into set of tuples
            for claim in revision_claim_list:
                numeric_item_id = int(item_id[1:])
                if claim[0] == numeric_item_id:
                    new_claims_set.add(tuple(claim))
                else:
                    print("Subject in triple {} != item_id {} in file {}: ".format(claim, item_id, revision_file))
                    return

            # Get insert operations (new_set - old_set)
            insert_operations = extract_item_triple_operations(new_claims_set, old_claims_set, revision_ts, "ins")
            item_triple_operations.extend(insert_operations)

            # Get delete operations (old_set - new_set)
            delete_operations = extract_item_triple_operations(new_claims_set, old_claims_set, revision_ts, "del")
            item_triple_operations.extend(delete_operations)

            # Switch new_claim_set to old_claim_set for the next iteration
            old_claims_set = new_claims_set
            new_claims_set = set()

    return item_triple_operations


def write_item_triple_operations_to_file(item_revision_file):
    # Collects all triple operations for a given item and persists them into a file.
    dump_filename = item_revision_file.parents[0].name
    item_revision_filename = item_revision_file.name

    processed_revisions_fld = Path.cwd() / "extraction_process_data" / "revision_files" / "processed_revision_files"
    processed_revisions_dump_sub_fld = processed_revisions_fld / dump_filename
    processed_revisions_dump_sub_fld.mkdir(parents=True, exist_ok=True)
    processed_rev_marker = processed_revisions_dump_sub_fld / "{}.processed".format(item_revision_filename)

    if processed_rev_marker.exists():
        print("Revision file {} already processed - skip file.".format(item_revision_filename))
    else:
        # Cut out item id QXXX from filename pattern <filedump_name>_QXXX.json.bz2
        item_id = item_revision_filename[item_revision_filename.find("Q"):item_revision_filename.find(".json")]
        item_triple_operations = get_triple_operations_list(item_revision_file)

        dump_sub_fld = Path.cwd() / "extraction_process_data" / "triple_operations" / dump_filename
        dump_sub_fld.mkdir(parents=True, exist_ok=True)
        output_filename = "{}.txt.bz2".format(item_id)
        output_file = dump_sub_fld / output_filename

        with bz2.open(output_file, mode="wt", encoding="UTF-8") as f:
            # triple_operation format -> [subject, object, predicate, operation_type, rev_ts]
            for op in item_triple_operations:
                line = "{} {} {} {} {}\n".format(op[0], op[1], op[2], op[3], op[4])
                f.write(line)

        processed_rev_marker.touch()


def extract_triple_operations_for_dump_revisions_folder(dump_revision_folder):
    # Gathers item revisions files contained in <dump_revision_folder> to extract the
    # triple operations for these items.

    print("Extract triple operations for dump folder {} at {}."
          .format(dump_revision_folder.name, get_current_timestamp()))

    item_revision_files_list = [file for file in dump_revision_folder.iterdir()
                                if file.is_file()
                                and not file.name.startswith("redirected_")]

    for item_revision_file in item_revision_files_list:
        write_item_triple_operations_to_file(item_revision_file)


def save_item_revision_to_json_file(dump_file_name, item_id, revision_dict, item_is_redirected):
    # Saves revision dict for a given item into a .json file.

    xml_dump_subfolder = Path.cwd() / "extraction_process_data" / "revision_files" / '{}'.format(dump_file_name)
    xml_dump_subfolder.mkdir(parents=True, exist_ok=True)

    output_filename = "redirected_{}_{}.json.bz2".format(dump_file_name, item_id) \
        if item_is_redirected else "{}_{}.json.bz2".format(dump_file_name, item_id)
    output_filepath = xml_dump_subfolder / output_filename

    # Catch cases in which the claim list is empty and no revisions for an entity have been stored before
    if (not output_filepath.exists()) and len(revision_dict["claims"]) == 0:
        return

    else:
        revision_json = json.dumps(revision_dict)
        with bz2.open(output_filepath, mode="at", encoding="UTF-8") as f:
            f.write(revision_json + "\n")

    # {
    #     "revision": "1010281398",
    #     "timestamp": "2019-09-08T18:43:46Z",
    #     "claims": [
    #         [
    #             <item_id>         "3964154",
    #             <predicate_id>    "17",
    #             <object_id>       "159"
    #         ],
    #         [
    #             "Q3964154",
    #             "P131",
    #             "Q2246"
    #         ],
    #         [
    #             "Q3964154",
    #             "P31",
    #             "Q13626398"
    #         ]
    #     ]
    # }


def get_truthy_claims_list(item_dict):
    statements = []

    if len(item_dict['claims']) > 0:

        item_id = item_dict["id"]
        claim_list_dict = item_dict["claims"]

        for proprty, claim_list in claim_list_dict.items():
            preferred_statements = []
            normal_statements = []

            for claim_dict in claim_list:
                rank = claim_dict["rank"].lower()  # rank is always in ['deprecated' | 'preferred' | 'normal']
                if rank != "deprecated":
                    mainsnak = claim_dict["mainsnak"]
                    snak_type = mainsnak["snaktype"].lower()

                    # In case "datavalue" is not contained in mainsnak --> mainsnak['snaktype'] = ['somevalue' | 'novalue']
                    if snak_type == "value" and "datavalue" in mainsnak:

                        # check if object is Wikidata ntity
                        # (In case mainsnak['datavalue']['type'] != 'wikibase-entityid'
                        # --> mainsnak['datavalue']['type'] in
                        # [ 'string'
                        # | 'monolingualtext'
                        # | 'time'
                        # | 'quantity'
                        # | 'globecoordinate'])
                        mainsnak_type = mainsnak['datavalue']['type']
                        if mainsnak_type == 'wikibase-entityid':

                            objct_dict = mainsnak['datavalue']['value']
                            objct_type = objct_dict['entity-type']

                            # check if object_type is 'item'
                            # (Otherwise it is a 'property')
                            if objct_type == "item":
                                # object_id = "Q{}".format(object_dict["numeric-id"])
                                triple = (int(item_id[1:]), int(proprty[1:]), int(objct_dict["numeric-id"]))

                                if rank == "preferred":
                                    preferred_statements.append(triple)
                                elif rank == "normal":
                                    normal_statements.append(triple)

                                # Check if numeric_id is always like id without the prefix "Q"
                                if "id" in objct_dict and str(objct_dict["numeric-id"]) != objct_dict["id"][1:]:
                                    print("Different ids for numeric-id {} and id {}".format(
                                        objct_dict["numeric-id"],
                                        objct_dict["id"][1:]))

            if preferred_statements:
                statements.extend(preferred_statements)
            else:
                statements.extend(normal_statements)

    return statements

    # Structure of item_dict
    #
    # {
    #     "type": "item",
    #     "id": "Q3918736",
    #     "labels": {
    #         "ru": {
    #             "language": "ru",
    #             "value": "Седов, Валентин Васильевич"
    #         },
    #         "be": {
    #             "language": "be",
    #             "value": "Валянцін Васілевіч Сядоў"
    #         },
    #         "lv": {
    #             "language": "lv",
    #             "value": "Valentīns Sedovs"
    #         }
    #     },
    #     "descriptions": [],
    #     "aliases": {
    #         "ru": [
    #             {
    #                 "language": "ru",
    #                 "value": "Седов В. В."
    #             },
    #             {
    #                 "language": "ru",
    #                 "value": "Валентин Васильевич Седов"
    #             },
    #             {
    #                 "language": "ru",
    #                 "value": "Седов Валентин Васильевич"
    #             }
    #         ],
    #         "be": [
    #             {
    #                 "language": "be",
    #                 "value": "Валянцін Васільевіч Сядоў"
    #             },
    #             {
    #                 "language": "be",
    #                 "value": "Валянцін Сядоў"
    #             }
    #         ],
    #         "lv": [
    #             {
    #                 "language": "lv",
    #                 "value": "Sedovs"
    #             }
    #         ]
    #     },
    #     "claims": {
    #         "P107": [
    #             {
    #                 "mainsnak": {
    #                     "snaktype": "value",
    #                     "property": "P107",
    #                     "hash": "5ad0e8cd324540512b927b581b5ec523db0b91fd",
    #                     "datavalue": {
    #                         "value": {
    #                             "entity-type": "item",
    #                             "numeric-id": 215627,
    #                             "id": "Q215627"
    #                         },
    #                         "type": "wikibase-entityid"
    #                     }
    #                 },
    #                 "type": "statement",
    #                 "id": "q3918736$DBFA7AF6-46D8-45F0-B04F-CD5597FCF58E",
    #                 "rank": "normal"
    #             }
    #         ]
    #     },
    #     "sitelinks": {
    #         "ruwiki": {
    #             "site": "ruwiki",
    #             "title": "Седов, Валентин Васильевич",
    #             "badges": []
    #         },
    #         "bewiki": {
    #             "site": "bewiki",
    #             "title": "Валянцін Васілевіч Сядоў",
    #             "badges": []
    #         },
    #         "lvwiki": {
    #             "site": "lvwiki",
    #             "title": "Valentīns Sedovs",
    #             "badges": []
    #         }
    #     }
    # }

    # Structure of item_dict["claims"]
    #
    # "claims": {
    #         "P107": [
    #             {
    #                 "mainsnak": {
    #                     "snaktype": "value",
    #                     "property": "P107",
    #                     "hash": "5ad0e8cd324540512b927b581b5ec523db0b91fd",
    #                     "datavalue": {
    #                         "value": {
    #                             "entity-type": "item",
    #                             "numeric-id": 215627,
    #                             "id": "Q215627"
    #                         },
    #                         "type": "wikibase-entityid"
    #                     }
    #                 },
    #                 "type": "statement",
    #                 "id": "q3918736$DBFA7AF6-46D8-45F0-B04F-CD5597FCF58E",
    #                 "rank": "normal"
    #             }
    #         ]
    #     }

    # Structure of claims[property_id][0]["mainsnak"]]
    #
    # 'mainsnak': {
    #     'snaktype': 'value',
    #     'property': 'P107',
    #     'hash': '5ad0e8cd324540512b927b581b5ec523db0b91fd',
    #     'datavalue': {
    #         'value': {
    #             'entity-type': 'item',
    #             'numeric-id': 215627,
    #             'id': 'Q215627'
    #         },
    #         'type': 'wikibase-entityid'
    #     }
    # }


def parse_xml_dump(dump_file):
    # Traverse a wikidata history xml dump line-wise and extract information about item revisions.

    print("Started processing file {} at {}.".format(dump_file.name, get_current_timestamp()))
    with bz2.open(dump_file, "rt", encoding="UTF-8") as xml_dump:

        item_id = None
        revision_id = None
        timestamp = None
        text = None
        claims = None
        item_dict = None
        format = None
        item_was_redirected = False

        for line in xml_dump:
            if line.startswith("    <title>"):
                item_id = line[len("    <title>"):-len("</title>\n")]

            if line.startswith("    <redirect title") and item_id.startswith("Q"):
                redir_target_item_id = line[len("    <redirect title ="):-len("\" />\n")]
                item_was_redirected = True
                create_log_entry_for_redirect_item(dump_file.name, item_id, redir_target_item_id)

            elif line.startswith("      <id>"):
                revision_id = line[len("      <id>"):-len("</id>\n")]
            elif line.startswith("      <timestamp>"):
                timestamp = line[len("      <timestamp>"):-len("</timestamp>\n")]
            elif line.startswith("      <comment>"):
                comment = line[len("      <comment>"):-len("</comment>\n")]
            elif line.startswith("      <format>"):
                format = line[len("      <format>"):-len("</format>\n")]
            elif line.startswith("      <text bytes"):
                if format == 'application/json' and item_id and item_id.startswith("Q"):
                    text = line[line.find('>') + 1: -len('</text>') - 1]

                    if len(text) > 0:
                        text = unescape(text)
                        item_dict = json.loads(text)

                        if 'type' in item_dict and 'id' in item_dict:
                            claim_triple_list = get_truthy_claims_list(item_dict)
                            revision_dict = create_item_revision_dict(item_id, revision_id, timestamp,
                                                                      claim_triple_list)
                            save_item_revision_to_json_file(dump_file.name, item_id, revision_dict, item_was_redirected)

            if line == "  </page>\n" or line == "    </revision>\n":
                revision_id = None
                timestamp = None
                format = None
                text = None
                item_dict = None
                claims = None
                claim_triple_list = None
                page_id = None
                item_id = None
                item_was_redirected = False
                redir_target_item_id = None
                redir_item_id = None
                revision_dict = None


def process_dump_file(file):
    # Parses Wikidata history xml dump and marks it as processed. This mechanism serves as
    # a checkpoint in case the long-running process is aborted in between.

    print("Process file {}\n".format(file.name))
    processed_xml_dumps_folder = file.parents[0] / "revisions_extracted_dumps"
    processed_xml_dumps_folder.mkdir(exist_ok=True)
    processed_marker = processed_xml_dumps_folder / "{}.processed".format(file.name)

    if processed_marker.exists():
        print("File {} already processed - Skip file.".format(file.name))
    else:
        parse_xml_dump(file)
        processed_marker.touch()


def collect_subfolder_triple_operations_to_file(dump_subfolder, q, redir_dict=None, filters=None):
    # Traverses a dump sub folder in which triple operations are stored per item to collect them into a single file.
    # In the collection process the filters of LaCroix et al.(2020) are applied and redirected items are dissolved
    # by attaching them to the item ids of their targets.

    subfolder_triple_ops = [file for file in dump_subfolder.iterdir() if file.is_file() and file.name.startswith("Q")]
    print("Get triple operations from {}.".format(dump_subfolder.name))

    triple_operations_folder = Path.cwd() / "extraction_process_data" / "triple_operations"
    processed_triple_ops_dump_subfld = triple_operations_folder / "processed_triple_operations" / dump_subfolder
    processed_triple_ops_dump_subfld.mkdir(parents=True, exist_ok=True)

    for triple_operations_log in subfolder_triple_ops:
        processed_triple_ops_marker = processed_triple_ops_dump_subfld / "{}.processed".format(
            triple_operations_log.name)

        if processed_triple_ops_marker.exists():
            print("Triple operations file {} already processed - Skip file.".format(triple_operations_log.name))
        else:
            output_lines = []
            with bz2.open(triple_operations_log, mode="rt", encoding="UTF-8") as item_triple_operations_file:
                for line in item_triple_operations_file:
                    subject_, object_, predicate_, operation_type, ts = line.split()

                    # Resolve redirects in obj
                    if redir_dict:
                        object_ = redir_dict.get(object_, object_)

                    # If filter is attached use it to only collect selected triples ops
                    if filters:
                        if not (subject_ in filters["filtered_entities"]
                                and object_ in filters["filtered_entities"]
                                and predicate_ in filters["filtered_relations"]) or (subject_ == object_):
                            continue

                    out_line = "{} {} {} {} {}\n".format(subject_, object_, predicate_, operation_type, ts)
                    output_lines.append(out_line)

                if output_lines:
                    q.put(output_lines)
                    processed_triple_ops_marker.touch()

    return "Finished gathering of triple extraction for folder {}.".format(dump_subfolder.name)


def writer(q, file):
    # Listens for messages on the q and writes to file.

    with bz2.open(file, mode="at", encoding="utf-8") as output:
        while 1:
            m = q.get()
            if m == 'kill':
                break
            output.writelines(m)
            output.flush()


def compile_triple_operations(num_cpu_cores):
    # Compile all synthesized triple operations into a single file.

    output_path = Path.cwd() / "extraction_process_data" / "compiled_triple_operations"
    output_path.mkdir(exist_ok=True)
    output_file = output_path / "compiled_triple_operations_filtered.txt.bz2"

    # Use Manager queue here to delegate writing into a single file from multiple jobs
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(num_cpu_cores)

    # Start writer process
    watcher = pool.apply_async(writer, (q, output_file))

    # Get filters of LaCroix (2020)
    filter_path = Path.cwd() / "extraction_process_data" / "filters"
    filters = {"filtered_entities": read_filter_file(filter_path / "entities_filtered_by_LaCroix_et_al_2020"),
               "filtered_relations": read_filter_file(filter_path / "predicates_filtered_by_LaCroix_et_al_2020")}

    # Load dict which maps source and target items in a redirect. Used for dissolving ids of redirected items.
    target_id_per_redirected_item = get_item_redirects_dict()

    # Path where triple ops are stored for each item
    triple_ops_path = Path.cwd() / "extraction_process_data" / 'triple_operations'
    triple_ops_dump_subfolders = [fld for fld in triple_ops_path.iterdir()
                                  if fld.is_dir() and not fld.name.startswith("processed_")]
    print("Found {} folders containing item triple operations.".format(len(triple_ops_dump_subfolders)))

    # Each subfolder is attached to a job
    jobs = []
    for subfolder in triple_ops_dump_subfolders:
        job = pool.apply_async(collect_subfolder_triple_operations_to_file,
                               (subfolder, q, target_id_per_redirected_item, filters))
        jobs.append(job)

    # Collect job results
    for job in jobs:
        result = job.get()
        print(result)

    # Kill the writer process
    q.put('kill')
    pool.close()
    pool.join()


def filter_compiled_triple_operations(items_filter_list, predicates_filter_list):
    compiled_triples_path = Path.cwd() / "extraction_process_data" / "compiled_triple_operations"
    raw_triples_file = compiled_triples_path / "compiled_triple_operations_raw.txt.bz2"

    with bz2.open(compiled_triples_path / "compiled_triple_operations_filtered.txt.bz2", "wt") as output:
        with bz2.open(raw_triples_file, mode="rt", encoding="UTF-8") as compiled_triple_operations_file:
            gathered_operations = 0
            total_operations = 0
            for line in compiled_triple_operations_file:
                subject_, object_, predicate_, op_type, ts = line.split()
                total_operations += 1
                if subject_ in items_filter_list and object_ in items_filter_list and predicate_ in predicates_filter_list:
                    output_line = "{} {} {} {} {}\n".format(subject_, object_, predicate_, op_type, ts)
                    output.write(output_line)
                    gathered_operations += 1

    print("Finished filtering process by selecting {} out of {} operations"
          .format(gathered_operations, total_operations))


def read_filter_file(file):
    filter_list = []
    with open(file) as f:
        for line in f:
            _, wikidata_id, name = line.split("\t")
            filter_list.append(wikidata_id)

    return filter_list


def remove_duplicates(triple_operations):
    index = 0
    consistent_triple_operations = []
    triple_state_dict = {}

    while index + 1 < len(triple_operations):
        curr_subjc, curr_objc, curr_pred, curr_op_type, curr_ts = triple_operations[index]
        curr_triple = (curr_subjc, curr_objc, curr_pred)

        next_subjc, next_objc, next_pred, next_op_type, next_ts = triple_operations[index + 1]
        next_triple = (next_subjc, next_objc, next_pred)

        # Handle duplicate triple operation (h,r,t,+,ts) --> (h,r,t,-,ts)
        if curr_triple == next_triple and curr_ts == next_ts and curr_op_type != next_op_type:
            index += 2
            continue

        # Handle first operation for a triple
        if curr_triple not in triple_state_dict:
            if curr_op_type == "-":
                print("Invalid triple operations pattern. First operation for {} is a deletion".format(curr_triple))


        # Handle duplicate triple operation (h,r,t,+,ts) --> (h,r,t,+,ts + 1)
        elif triple_state_dict[curr_triple] == curr_op_type:
            index += 1
            continue

        triple_state_dict[curr_triple] = curr_op_type
        consistent_triple_operations.append(triple_operations[index])
        index += 1

    return consistent_triple_operations


def sort_filtered_triple_operations(input_file_name, compress_output=False):
    print("Load filtered triple operations.")
    compiled_triples_path = Path.cwd() / "extraction_process_data" / "compiled_triple_operations"
    input_file = compiled_triples_path / input_file_name

    # Get triple operations from file
    triple_operations = []
    with bz2.open(input_file, mode="rt", encoding="UTF-8") as f:
        for line in f:
            triple_operations.append(line.split())

    # Sort triple operations with respect to timestamp, triple, op_type
    triple_operations = sorted(triple_operations, key=operator.itemgetter(4, 0, 1, 2, 3))

    # Remove duplicates that occur after dissolving redirects
    triple_operations = remove_duplicates(triple_operations)

    print("Save sorted list to file.")
    output_path = Path.cwd() / "datasets"
    output_file_name = "Wikidata9M"

    if compress_output:
        output_file_name = output_file_name + ".txt.bz2"
        sorted_triple_ops_file = output_path / output_file_name
        f = bz2.open(sorted_triple_ops_file, mode="wt", encoding="UTF-8")
    else:
        output_file_name = output_file_name + ".txt"
        sorted_triple_ops_file = output_path / output_file_name
        f = sorted_triple_ops_file.open(mode="wt", encoding="UTF-8")

    # triple_operation format : [subject, object, predicate, operation_type, rev_ts]
    for index, op in enumerate(triple_operations):
        line = "{} {} {} {} {}".format(op[0], op[1], op[2], op[3], op[4])
        f.write(line + "\n")
    f.close()


def get_dump_list(wikidata_history_dump_date):
    xml_dumps_download_path = Path.cwd() / "extraction_process_data" / "xml_dumps_{}".format(wikidata_history_dump_date)
    xml_dumps_file_pattern = re.compile(r"[\s\S]*pages-meta-history.*\.bz2$$")

    xml_dump_file_list = [xml_dump for xml_dump in xml_dumps_download_path.iterdir()
                          if xml_dump.is_file() and xml_dumps_file_pattern.match(xml_dump.name)]

    return xml_dump_file_list


def get_revision_folders_list():
    revision_files_path = Path.cwd() / "extraction_process_data" / "revision_files"
    revision_folder_pattern = re.compile(r'[\s\S]*pages-meta-history.*\.bz2$$')
    revision_folder_list = [rev_folder for rev_folder in revision_files_path.iterdir()
                            if rev_folder.is_dir() and revision_folder_pattern.match(rev_folder.name)]

    return revision_folder_list


def main():
    # Obtain number of CPU cores
    num_of_cores_available = os.cpu_count()
    input_message = "{} CPU cores available. How many do you want to apply?".format(num_of_cores_available)
    num_of_cores_granted = int(input(input_message))

    input_message = "Enter your dump date (YYYYMMDD)."
    wikidata_dump_date = input(input_message)
    wikidata_path = Path.cwd() / "extraction_process_data"
    print("Output path is: {}.".format(wikidata_path))

    print("Download XML history dumped at {}.".format(wikidata_dump_date))
    # download_wikidata_history_dumps(wikidata_dump_date)

    print("Extract revision information from downloaded XML dumps...")
    xml_dump_file_list = get_dump_list(wikidata_dump_date)
    with ProcessPoolExecutor(max_workers=num_of_cores_granted) as executor:
        for xml_file, _ in zip(xml_dump_file_list, executor.map(process_dump_file, xml_dump_file_list)):
            print('File {} has been processed successfully: {}'.format(xml_file.name, get_current_timestamp()))

    print("Extract triple operations from json revision files.")
    json_revision_folder = get_revision_folders_list()
    with ProcessPoolExecutor(max_workers=num_of_cores_granted) as executor:
        for folder, _ in zip(json_revision_folder,
                             executor.map(extract_triple_operations_for_dump_revisions_folder, json_revision_folder)):
            print('Finished processing folder {} at {}'.format(folder.name, get_current_timestamp()))

    print("Start compilation of triple operations.")
    compile_triple_operations(num_of_cores_granted)
    print("Finished compilation at {}.".format(get_current_timestamp()))

    print("Sort and save triple operations.")
    sort_filtered_triple_operations(input_file_name="compiled_triple_operations_filtered.txt.bz2", compress_output=True)


if __name__ == '__main__':
    main()
