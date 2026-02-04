import json

def convert_to_jsonl(input_filename, output_filename):
    with open(input_filename, 'r') as f:
        # Load the file. Handle both single objects or arrays.
        content = f.read().strip()
        if content.startswith('['):
            data = json.loads(content)
        else:
            # If multi-object but not in an array, we wrap it to load it
            data = json.loads(f"[{content.replace('}{', '},{')}]")

    with open(output_filename, 'w') as f:
        for entry in data:
            # Write each object as a single line
            f.write(json.dumps(entry) + '\n')

convert_to_jsonl('dummy_claims_data.jsonl', 'dummy_claims_data_fixed.jsonl')

