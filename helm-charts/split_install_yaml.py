import os
import re

input_file = 'temp/install.yaml'
output_dir = 'kafka-ops-operator/templates'
crds_dir = 'kafka-ops-operator/crds'
os.makedirs(output_dir, exist_ok=True)
os.makedirs(crds_dir, exist_ok=True)

with open(input_file, 'r') as f:
    content = f.read()

docs = [doc.strip() for doc in content.split('---') if doc.strip()]

def extract(doc, key):
    # Simple regex to extract top-level key: value
    match = re.search(rf'^{key}:\s*(\S+)', doc, re.MULTILINE)
    return match.group(1) if match else 'unknown'

def extract_name(doc):
    # Look for metadata:\n  name: value
    match = re.search(r'^metadata:\n(?:  .*\n)*  name:\s*(\S+)', doc, re.MULTILINE)
    return match.group(1) if match else 'noname'

for idx, doc in enumerate(docs):
    kind = extract(doc, 'kind')
    name = extract_name(doc)
    directory = crds_dir if kind == 'CustomResourceDefinition' else output_dir
    filename = f'{idx:03d}-{kind}-{name}.yaml'
    with open(os.path.join(directory, filename), 'w') as out:
        out.write(doc + '\n')

    print(f'Wrote {filename} to {directory}')