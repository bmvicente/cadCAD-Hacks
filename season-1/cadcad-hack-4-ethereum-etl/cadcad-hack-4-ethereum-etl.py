#!/usr/bin/env python
# coding: utf-8

# <span style="display:block;text-align:center;margin-right:105px"><img src="../media/logos/hacks-logo.png" width="200"/></span>

# ## Table of Contents
# 
# <ul>
#     <li><a href='#Dependencies'>Dependencies</a></li>
#     <li><a href='#Setup-/-Preparatory-Steps'>Setup / Preparatory Steps</a>
#         <ul style='margin-top: 0em;'>
#             <li><a href='# Download the ETH daily supply timeseries from the Ethereum-ETL dataset'>Download the ETH daily supply timeseries from the Ethereum-ETL dataset</a></li>
#         </ul>
#     </li>
#     <li><a href='#Modelling'>Modelling</a>
#         <ol style='margin-top: 0em;'>
#             <li><a href='#1.-State-Variables'>State Variables</a></li>
#             <li><a href='#2.-System-Parameters'>System Parameters</a></li>
#             <li><a href='#3.-Policy-Functions'>Policy Functions</a></li>
#             <li><a href='#4.-State-Update-Functions'>State Update Functions</a></li>
#             <li><a href='#5.-Partial-State-Update-Blocks'>Partial State Update Blocks</a></li>
#         </ol>
#     </li>
#     <li><a href='#Simulation'>Simulation</a>
#         <ol style='margin-top: 0em;' start="6">
#             <li><a href='#6.-Configuration'>Configuration</a></li>
#             <li><a href='#7.-Execution'>Execution</a></li>
#             <li><a href='#8.-Output-Preparation'>Output Preparation</a></li>
#             <li><a href='#9.-Analysis'>Analysis</a></li>
#         </ol>
#     </li>
# </ul>

# ---

# # Dependencies

# In[1]:


# cadCAD standard dependencies

# cadCAD configuration modules
from cadCAD.configuration.utils import config_sim
from cadCAD.configuration import Experiment

# cadCAD simulation engine modules
from cadCAD.engine import ExecutionMode, ExecutionContext
from cadCAD.engine import Executor

# cadCAD global simulation configuration list
from cadCAD import configs

# Included with cadCAD
import pandas as pd


# In[6]:


# Additional dependencies

# For parsing the data from the API
import json
# For downloading data from API
import requests as req
# For generating random numbers
import math
# For visualization
import plotly.express as px
# For Google BigQuery authentication
from google.oauth2 import service_account


# # Setup / Preparatory Steps
# 
# ## Download the ETH daily supply timeseries from the Ethereum-ETL dataset

# In[7]:


get_ipython().run_cell_magic('capture', '', '\ncredentials = service_account.Credentials.from_service_account_file(\n    \'./cadcad-edu-f0c5ddda00cc.json\',\n)\n\n# Ether Supply by Date Query\n# More examples at\n# https://github.com/blockchain-etl/awesome-bigquery-views\nQUERY = """\n/*\nThis query returns a time-series of the total ETH supply\nover time, with daily granularity.\n*/\n\n-- \n/* \nRetrieve the ETH reward per date\nby summing the genesis / reward traces\n*/\nwith ether_emitted_by_date  as (\n  select date(block_timestamp) as date, \n  sum(value) as value /* Unit: attoETH */\n  from `bigquery-public-data.crypto_ethereum.traces`\n  /* Get only Genesis and Reward traces */\n  where trace_type in (\'genesis\', \'reward\') \n  group by date(block_timestamp)\n)\n--\n/* \nTake the cumulative sum of the Daily ETH reward\nand convert it to ETH units\n*/\nselect date, \nsum(value) OVER (ORDER BY date) / power(10, 18) AS supply\nfrom ether_emitted_by_date\nwhere date > "2015-01-01"\n\n"""\n\n# Send the SQL query to the ethereum-etl dataset\n# on Google BigQuery.\n# Requires the pandas-gbq library\nsupply_data = pd.read_gbq(QUERY, project_id="cadcad-edu", credentials=credentials)\n\n# Print the last 5 rows\nsupply_data.tail(5)')


# # Modelling

# ## 1. State Variables

# In[8]:


initial_state = {
    'timestamp': None, # Current time
    'supply': 0, # ETH supply
}
initial_state


# ## 2. System Parameters

# In[9]:


# Transform the data dataframe into a 
# {timestep: data} dictionary
data_dict = supply_data.to_dict(orient='index')

system_params = {
    'supply_timeseries': [data_dict]
}

# Element for timestep = 3
system_params['supply_timeseries'][0][3]


# ## 3. Policy Functions

# In[10]:


def p_parse_data(params, substep, state_history, previous_state):
    """
    Parse the data from the current timestep
    """
    t = previous_state['timestep']
    
    # Data for this timestep
    ts_data = params['supply_timeseries'][t]
    
    # Parse the current timestamp by using Unix epochs as convention
    timestamp = ts_data['date']
    supply = ts_data['supply']
    
    
    return {'timestamp': timestamp,
            'supply': supply}


# ## 4. State Update Functions

# In[11]:


def generic_assign_state_update(variable):
    """
    Create a State Update Function that assigns the state variable
    given by the 'variable' argument
    with the value given by the policy input
    given by the same name.
    """
    def state_update(params, substep, state_history, previous_state, policy_input):
        return (variable, policy_input[variable])
    return state_update


# ## 5. Partial State Update Blocks

# In[12]:


partial_state_update_blocks = [
    {
        'policies': {
            'parse_timestep_data': p_parse_data
        },
        'variables': {
            'timestamp': generic_assign_state_update('timestamp'),
            'supply': generic_assign_state_update('supply')
        }
    }
]


# # Simulation

# ## 6. Configuration

# In[13]:


sim_config = config_sim({
    "N": 1, # the number of times we'll run the simulation ("Monte Carlo runs")
    "T": range(len(supply_data)), # the number of timesteps the simulation will run for
    "M": system_params # the parameters of the system
})


# In[14]:


del configs[:] # Clear any prior configs


# In[15]:


experiment = Experiment()
experiment.append_configs(
    initial_state = initial_state,
    partial_state_update_blocks = partial_state_update_blocks,
    sim_configs = sim_config
)


# ## 7. Execution

# In[16]:


exec_context = ExecutionContext()
simulation = Executor(exec_context=exec_context, configs=configs)
raw_result, tensor_field, sessions = simulation.execute()


# ## 8. Output Preparation

# In[17]:


simulation_result = pd.DataFrame(raw_result)
simulation_result.head()


# ## 9. Analysis

# In[18]:


# Plot the supply over time
px.line(simulation_result,
        x='timestamp',
        y='supply')

