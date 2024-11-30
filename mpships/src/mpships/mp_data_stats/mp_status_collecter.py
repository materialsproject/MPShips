import json
from collections import defaultdict
from mp_api.client import MPRester
from monty.serialization import loadfn, dumpfn
import os


if not os.path.exists('./output/elemental'):
    os.makedirs('./output/elemental')


API_KEY = "API_KEY" # replace to your MP API key

api_list = ['absorption', 'bonds', 'dielectric', 'elasticity', 
            'insertion_electrodes', 'electronic_structure', 'electronic_structure_bandstructure', 
            'electronic_structure_dos', 'magnetism', 'oxidation_states', 'piezoelectric', 
            'summary', 'thermo']

api_list = ['absorption']


count_dict = dict()
with MPRester(API_KEY) as mpr:
    for api_name in api_list:
        try:
            api_class = getattr(mpr.materials, api_name)
            docs = api_class.search(fields=['task_id', 'elements'])
            dumpfn(docs, f"./output/{api_name}.csv")
            count_dict[api_class.__class__.__name__] = len(docs)
            
            # update element count in material
            # element_dict = collections.defaultdict(int)
            element_str_dict = defaultdict(int)
            element_element_str_dict = defaultdict(lambda: defaultdict(int))
            for doc in docs:
                for element in doc.elements:
                    element_str_dict[element.name] += 1
                    # element_dict[element] += 1
                    for element2 in doc.elements:
                        if element.name != element2.name:
                            element_element_str_dict[element.name][element2.name] += 1

            
            with open(f"./output/count_ele_{api_name}.json", "w") as json_file:
                json.dump(element_str_dict, json_file)
            with open(f"./output/elemental/count_ele_ele_{api_name}.json", "w") as json_file:
                json.dump(element_element_str_dict, json_file)

            
            del docs
        except:
            print(api_name)
with open("./output/count.json", "w") as json_file:
    json.dump(count_dict, json_file)
