import requests
import json

# Function to create a new classification
def create_classification(classification_name, classification_description):
    url = "http://localhost:21000/api/atlas/v2/types/typedefs"

    # Classification definition
    classification_def = {
        "classificationDefs": [
            {
                "category": "CLASSIFICATION",
                "name": classification_name,
                "description": classification_description,
                "typeVersion": "1.0",
                "attributeDefs": []
            }
        ]
    }

    # Send the request
    response = requests.post(url, data=json.dumps(classification_def), headers={"Content-Type": "application/json"}, auth=('admin', 'admin'))
    
    return response.json()

# Example: Create a new classification
classification_name = "SensitiveData"
classification_description = "Data that contains sensitive information."

# Call the function
classification_response = create_classification(classification_name, classification_description)

print(f"Classification Creation Response: {classification_response}")
