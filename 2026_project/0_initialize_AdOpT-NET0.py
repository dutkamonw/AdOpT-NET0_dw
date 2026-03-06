import adopt_net0 as adopt
from pathlib import Path


## Initializ                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            e AdOpT-NET0
# Use the folder where this script is located
script_dir = Path(__file__).parent
path = script_dir / "inputs"
path.mkdir(parents=True, exist_ok=True)

# Create optimization templates in the inputs folder
adopt.create_optimization_templates(path)

# Create input data folder template in the inputs folder
adopt.create_input_data_folder_template(path)


## 
