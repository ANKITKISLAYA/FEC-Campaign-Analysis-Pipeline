import yaml

def load_config(file_path = 'config/pipeline_config.yaml'):
    """
    Load a YAML file and return its content.
    
    Args:
        file_path (str): The path to the YAML file.
        
    Returns:
        dict: The content of the YAML file as a dictionary.
    """
    with open(file_path, 'r') as file:
        config_yml = yaml.safe_load(file)
    return config_yml

if __name__ == "__main__":
    load_config()