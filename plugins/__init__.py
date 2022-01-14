import sys
import os

# init current dir
dir = os.path.dirname(os.path.realpath(__file__))

# add operator dependencies manually to sys path
sys.path.insert(0, dir + "/utils_operators" )
sys.path.insert(0, dir + "/ckan_operators" )