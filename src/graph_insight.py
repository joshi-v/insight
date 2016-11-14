import csv
import os
import collections
import Queue
from collections import defaultdict
from Queue import Queue
import sys
from os import path

class Graph(object):

    def __init__(self, graph_dict=None):
        """ initializes a graph object 
            If no dictionary or None is given, an empty dictionary will be used
        """
        if graph_dict == None:
            graph_dict = defaultdict(set)
        self.__graph_dict = graph_dict


    def vertices(self):
        """ returns the vertices of a graph """
        return list(self.__graph_dict.keys())

    def edges(self):
        """ returns the edges of a graph """
        return self.__generate_edges()

    def add_vertex(self, vertex):
        """ If the vertex "vertex" is not in 
            self.__graph_dict, a key "vertex" with an empty
            list as a value is added to the dictionary. 
            Otherwise nothing has to be done. 
        """
        if vertex not in self.__graph_dict:
            self.__graph_dict[vertex]

    def add_edge(self, edge):
        """ assumes that edge is of type set, tuple or list; 
            between two vertices can be multiple edges! 
        """
        edge = set(edge)
        vertex1 = edge.pop()
        if edge:
            # not a loop
            vertex2 = edge.pop()
        else:
            # a loop
            vertex2 = vertex1

        self.__graph_dict[vertex1].add(vertex2)
        self.__graph_dict[vertex2].add(vertex1)


    def __generate_edges(self):
        """ A static method generating the edges of the 
            graph "graph". Edges are represented as sets 
            with one (a loop back to the vertex) or two 
            vertices 
        """
        edges = []
        for vertex in self.__graph_dict:
            for neighbour in self.__graph_dict[vertex]:
                if {neighbour, vertex} not in edges:
                    edges.append({vertex, neighbour})
        return edges

    def __str__(self):
        res = "vertices: "
        for k in self.__graph_dict:
            res += str(k) + " "
        res += "\nedges: "
        for edge in self.__generate_edges():
            res += str(edge) + " "
        return res


    def bfs(self, start, end):
        if end in self.__graph_dict[start]:
            return 1
        if start not in self.__graph_dict or end not in self.__graph_dict:
            self.add_edge((start, end))
            return float('inf')
        edge_to = {}
        marked = set()  # contains vertices which have been found
        q = Queue()  # contains the vertices to iterate over
        marked.add(start)  # mark start as found
        q.put(start)  # add start to queue
        while not q.empty():
            vertex = q.get(block=False)
            for v in self.__graph_dict[vertex]:
                if v not in marked:
                    marked.add(v)
                    q.put(v)
                    edge_to[v] = vertex
        return self._trusted_or_unverified(marked, edge_to, start, end)

    def _trusted_or_unverified(self, marked, edge_to, start, end):
        if end not in marked:
            self.add_edge((start, end))
            return float('inf')
        path = []
        current = end
        while current != start:
            path.append(current)
            current = edge_to[current]

        path.append(current)

        return len(path)

    def validate_friends(self):
        try:  # to open file and exit if file does not exist with appropriate message
            fout1 = open('./paymo_output/output1.txt', 'w') # length is 2, direct connect
        except IOError:
            print ('Could not open output1.txt file')
            sys.exit()
        try:  # to open file and exit if file does not exist with appropriate message
            fout2 = open("./paymo_output/output2.txt", "w") # len 3
        except IOError:
            print ('Could not open output2.txt file')
            sys.exit()
        try:  # to open file and exit if file does not exist with appropriate message
            fout3 = open("./paymo_output/output3.txt", "w") # len 5
        except IOError:
            print ('Could not open output3.txt file')
            sys.exit()
        try:  # to open file and exit if file does not exist with appropriate message
            f = open('./paymo_input/stream_payment.txt', 'r')
            f.readline() # read and ignore header
        except IOError:
            print ('Could not open stream_payment.txt file')
            sys.exit()
        for line in f:
            row = line.split(',')
            v1 = int(row[1].strip())
            v2 = int(row[2].strip())
            val = self.bfs(v1, v2)
            # will eventually remove next line
            if val <= 2:
                fout1.write("trusted" + "\n")
                fout2.write("trusted" + "\n")
                fout3.write("trusted" + "\n")
            elif val == 3:
                fout1.write("unverified" + "\n")
                fout2.write("trusted" + "\n")
                fout3.write("trusted" + "\n")
            elif val == 4:
                fout1.write("unverified" + "\n")
                fout2.write("unverified" + "\n")
                fout3.write("trusted" + "\n")
            elif val == 5:
                fout1.write("unverified" + "\n")
                fout2.write("unverified" + "\n")
                fout3.write("trusted" + "\n")
            else:
                fout1.write("unverified" + "\n")
                fout2.write("unverified" + "\n")
                fout3.write("unverified" + "\n")

        f.close()
        fout1.close()
        fout2.close()
        fout3.close()

    def print_graph_dict(self):
        print (self.__graph_dict)


def read_file():

      friends = defaultdict(set)
      try:
        f = open('./paymo_input/batch_payment.txt', 'r')
        f.readline() # ignore header
      except IOError:
        print("Could not open batch_payment.txt file ")
        sys.exit()

      for line in f:
        row = line.split(',') # add both sides to make graph bidirectional
        v1 = int(row[1].strip())
        v2 = int(row[2].strip())
        friends[v1].add(v2)
        friends[v2].add(v1)

      f.close()
      return friends

if __name__ == "__main__":

    graph = Graph(read_file())
    graph.validate_friends()
