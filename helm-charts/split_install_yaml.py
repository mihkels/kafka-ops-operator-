import os
import re

input_file = 'dist/install.yaml'
output_dir = 'dist/split'
os.makedirs(output_dir, exist_ok=True)

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
    filename = f'{idx:03d}-{kind}-{name}.yaml'
    with open(os.path.join(output_dir, filename), 'w') as out:
        out.write(doc + '\n')
    print(f'Wrote {filename}')