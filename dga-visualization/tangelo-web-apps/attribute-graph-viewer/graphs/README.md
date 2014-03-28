# Attribute Graph Viewer

This project is a simple graph viewer intended to build medium sized ( < 2000 nodes ) bi partite attribute graphs.

# Features

-pan/zoom
-select a node type to list / hightlight all ndoes of that type
-hover over node to view its text
-click a node to show text for it and all connected nodes
-show all node text


##Requirements

Tangelo :  http://kitware.github.io/tangelo/#

Python 2.7.x



## Set up


###Install tangelo 

The following steps walk you through setting up tangelo to serve a web app on your system.  If you are only serving on localhost some steps are not required.

> pip install tangelo

Local your tangelo home directory.  It should be under your python install,

> PYTHON_HOME/share/tangelo/


create a new directory named "graphviewer" under tangelo/web.  Copy all of the files for this example to tangelo/web/graphviewer



### Start the web service

The python scripts in this example use relitive file paths, so you must start tangelo from the project directory.
Execute from tangelo/web/graphviewer

> tangelo start

In your web browser open localhost:8080/graphviewer


### Example data

a_tiny_example:  a small example with just a few nodes and 3 types to show the basic functionality.

facebook examples:  some simple but slightly larger graphs. data from http://snap.stanford.edu/data/egonets-Facebook.html
facebook_0_circles: A single face book users circles.  user nodes are conntected to the circles for which they are members.
facebook_0_edges: The friendship network from a single user.  users who are friends share an edge.  This is an example of a small number of nodes with dense edges.
facebook_multi_cirlces:  facebook user circles of 3 different users.  Each users circles are broken out to their own node type.


