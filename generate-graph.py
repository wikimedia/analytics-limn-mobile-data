# Tool to generate appropriate files in the datasources and graphs directory
# TOUSE:
import argparse
import json
import copy
import yaml
import io


def make_datasource(name):
    try:
        f = open("datafiles/%s.csv" % name, "r")
        headers = f.readlines()[0]
        columns = headers.split(',')
        f.close()
        ds_columns = []
        for i in range(len(columns)):
            column = {}
            if i == 0:
                column["type"] = "date"
            else:
                column["type"] = "int"
            column["label"] = columns[i]
            ds_columns.append(column)
        
        ds = {
            "timespan": {
                "start": "2013/05/0",
                "step": "1d",
                "end": "2013/03/19"
            },
            "url": "http://stat1001.wikimedia.org/limn-public-data/mobile/datafiles/%s.csv" % name,
            "format": "csv",
            "type": "timeseries",
            "id": name,
            "columns": ds_columns
        }
        print "Writing datasource..."
        f = open("datasources/%s.json" % name, "w")
        dump = json.dumps(ds, indent=4)
        f.writelines(dump)
        f.close()
        return True
    except IOError:
        print "First you must run:\npython generate.py -c scripts/config.yaml mobile -g %s" % name
        return False


def get_graph_config(name):
    config_path = 'mobile/config.yaml'
    with io.open(config_path, encoding='utf-8') as config_file:
        config = yaml.load(config_file)
    return config["graphs"][name]


def get_datasource(name):
    f = open("datasources/%s.json" % name, "r")
    jsonds = '\n'.join(f.readlines())
    ds = json.loads(jsonds)
    f.close()
    return ds


def generate_graph(name):
    config = get_graph_config(name)
    ds = get_datasource(name)
    ds_columns = ds["columns"]
    
    graph_columns = []
    for i in range(len(ds_columns)):
        if i > 0:
            ds_column = ds_columns[i]
            column = {
                "disabled": False,
                "index": 0,
                "metric": {
                    "source_id": name,
                    "type": ds_column["type"],
                    "source_col": i
                },
                "nodeType": "line",
                "options": {
                    "stroke": {
                        "width": 2
                    },
                    "label": ds_column["label"],
                    "noLegend": False,
                    "dateFormat": "MMM YYYY"
                }
            }
            graph_columns.append(column)

    axis = {"disabled": False, "nodeType": "axis"}
    axis_x = copy.deepcopy(axis)
    axis_y = copy.deepcopy(axis)
    axis_x["options"] = {
        "tickFormat": "MMM YY",
        "dimension": "x",
        "orient": "bottom"
    }
    axis_y["options"] = {
        "tickFormat": "MMM YY",
        "dimension": "y",
        "orient": "left"
    }
    grid = {
        "disabled": False,
        "nodeType": "grid",
        "options": {
            "ticks": 10,
            "dimension": "x"
        }
    }
    grid_x = grid
    grid_y = copy.deepcopy(grid)
    grid_y["options"]["dimension"] = "y"

    callout = {
        "nodeType": "callout",
        "target": "latest",
        "disabled": False,
        "steps": [
            "1y",
            "1M"
        ],
        "metricRef": 0,
        "options": {
            "deltaPercent": True,
            "dateFormat": "MMM YYYY",
            "colorDelta": True
        }
    }
    legend = {
        "disabled": False,
        "nodeType": "legend",
        "options": {
            "valueFormat": ",.2s",
            "dateFormat": "MMM YYYY"
        },
        "label": "Aug 2012"
    }
    zoom_brush = {
        "disabled": False,
        "nodeType": "zoom-brush",
        "options": {
            "allowY": True,
            "allowX": True
        }
    }
    graph_json = {
        "graph_version": "0.6.0",
        "name": config["title"],
        "root": {
            "scaling": "linear",
            "nodeType": "canvas",
            "minWidth": 750,
            "minHeight": 500,
            "height": 500,
            "disabled": False,
            "width": "auto",
            "children": [
                axis_x, axis_y, grid_x, grid_y, callout, legend, zoom_brush,
                {
                    "disabled": False,
                    "nodeType": "line-group",
                    "options": {
                        "palette": "wmf_projects",
                        "scale": "log",
                        "stroke": {
                            "opacity": 1,
                            "width": 2
                        },
                        "dateFormat": "MMM YYYY"
                    },
                    "children": graph_columns
                }
            ],
            "id": name
        }
    }
    print "Writing graph"
    f = open("graphs/%s.json" % name, "w")
    dump = json.dumps(graph_json, indent=4)
    f.writelines(dump)
    f.close()


def generate(name):
    success = make_datasource(name)
    if success:
        generate_graph(name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate data for the mobile dashboard.')
    parser.add_argument('-g', '--graph', help='the name of a single graph you want to generate for')
    args = parser.parse_args()
    generate(args.graph)
