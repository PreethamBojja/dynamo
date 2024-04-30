import os
import re

# Initialize a dictionary to count each type of value
value_counts = {}
success_count = 0 
error_count = 0  # Initialize error count
N = 50

# Loop through each experiment
for i in range(N):
    print("Experiment: {}".format(i + 1))
    os.chdir('../dynamo')
    stream = os.popen('mix test test/dynamo_test.exs')
    output = stream.read()
    
    # Extract and count "GET: value" lines from the output
    for line in output.split('\n'):
        match = re.search(r'GET\s*:\s*"(\w+)"', line)
        if match:
            success_count = success_count + 1
            value = match.group(1)
            value_counts[value] = value_counts.get(value, 0) + 1
    
error_count = N - success_count

# Print the counts for each type of value
for value, count in value_counts.items():
    print("Value: {}, Count: {}".format(value, count))

print("Errors: {}".format(error_count))  # Print total error count
