/*jslint browser: true, unparam: true */

/*globals tangelo, $, d3, SWG */

var highlight_options = []



/*
    Calls the python service to list all
    graphs in the projects graphs directory,
    populates the selection list, and
    loads the first graph.
    
    called by window.onload 
*/
function list_graphs(){
  $.ajax({
    url: 'graphservice/list',
    data: {},
    dataType: 'text',
    success: function(response) {
      graphs = jQuery.parseJSON(response)
      d3.select("#graph_select").selectAll("option").remove()
      d3.select("#graph_select")
			.on("change", change_graph)
			.selectAll("option")
            .data( graphs).enter()
            .append("option")
            .text(function (d) { return d; }); 
      change_graph() 
      
    },
    error: function (jqxhr, textStatus, reason) {
      alert("error "+textStatus+" "+reason)
    }
  })
}



/*
  Called whenever a graph is selected to load and draw
  the new graph.
*/
function change_graph(){
  var name = d3.select("#graph_select").property('value')
  $.ajax({
    url: 'graphservice/get',
    data: {'name':name},
    success: function(response) { 
      var graph = jQuery.parseJSON(response)
       $("#dialog").dialog().dialog("close")
 	   SWG.drawGraph('node_graph',graph)
       SWG.show_base_legend()
       populate_highlights()
    },
    error: function (jqxhr, textStatus, reason) {
      alert("error "+textStatus+" "+reason)
    }
  })
}


/*
   Override the SWG.node_text_func to change
   the node text that is displayed.  
   
   Default behavior is to display the node name the same as here.
   We override here for example purposes only
*/
SWG.node_text_func = function(d) {
  return d.name
}



/*
  Populate the highlight selection box
*/
function populate_highlights(){
   highlight_options = ['none']
   highlight_options_text = ['none']
   for (var type in SWG.node_types){
     highlight_options.push(type)
	 var count = SWG.node_types[type]['count']
	 var text = type
	  highlight_options_text.push(text+" ("+count+")")
   }
   
   d3.select("#highlights").selectAll("option").remove()
   d3.select("#highlights")
			.on("change", change_highlight)
			.selectAll("option")
            .data( highlight_options_text).enter()
            .append("option")
            .text(function (d) { return d; });    
}



/*
  When the highlight selection changes,
  re-color the graph and show
  a dialog box listing the highlighted terms
*/
function change_highlight(){
  $("#dialog").dialog().dialog("close")
  var sel = d3.select("#highlights").node()
  var index = sel.selectedIndex
  selected_term = highlight_options[index]
  
  // Default coloring
  if (index == 0) { 
	SWG.defaultColors()
  }
  else{ // highlight node type
	SWG.hightlightType(selected_term)
	SWG.showTypeDialog(selected_term)
  }
  
}


/*
 Load the UI controls
*/
window.onload = function () {   
    // Create control panel.
    $("#control-panel").controlPanel();

    // Enable the popover help items.
    //
    // First create a config object with the common options present.
    var popover_cfg;
    popover_cfg = {
            html: true,
            container: "body",
            placement: "top",
            trigger: "hover",
            title: null,
            content: null,
            delay: {
                show: 100,
                hide: 100
            }
    };

    // Dataset pulldown help.
    popover_cfg.content = "<b>Select a Graph:</b><br><br>" +
            "Choose a graph form the list.";
    $("#search_help").popover(popover_cfg);

	list_graphs()	
};

