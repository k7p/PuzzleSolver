from pyspark import SparkContext
import Sliding, argparse
import hashlib

  
def bfs_map(value):
    """ only looking at board state at a time and making children for it"""
    
    children_list = []
    children_list.append((value[0], value[1]))   

    #append the key of the tuple and the global variable
    #if input level is current level, make children  
    #append input and children   
    if value[1] == level:               #key,value is just the level
        ch = Sliding.children(WIDTH, HEIGHT, value[0]) #value[0] is the board 
        
        if ch:   
            for child in ch:
                children_list.append((child, level+1))

        #append tuple 
        
        #return (children, level+1)
    return children_list



def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    #pass # delete this line   
    minimum = min(value1, value2)
    return minimum


    #if reduce returns level that is not current level, we are done

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """

    #how can you get the children on the highest level from the solution?

    #how do you get a list of board states




    '''

    Get board and level from the RDD and pass into map and rduce

    reduceByKey(func)

    List of all children board states and pass into map

    while level < height - 1?????; do map and reduce
    '''




    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)

    #curr_level = 0



    """ YOUR MAP REDUCE PROCESSING CODE HERE """  

    #children = Sliding.children(WIDTH, HEIGHT, sol)

    #create the rdd

    rdd = sc.parallelize( [(sol, 0)] )

    #parallelize only takes in a list
    #you pass in the board because you start from there 

    #rdd = hash(rdd)
    #rdd = rdd.partitionBy(116)         

    rdd = rdd.coalesce(4)            
    rdd = rdd.partitionBy(16)           
    #rdd = hash(rdd)  

    while True: 
        #if level%4==0:
            #rdd = rdd.partitionBy(16, hash) 

        curr_size = rdd.count() #tells you how many things are in the rdd

        rdd = rdd.flatMap(bfs_map) #you want to return a list for reduce to reduce    

        rdd = rdd.reduceByKey(bfs_reduce) 

        if rdd.count() == curr_size:  #added nothing new so break 
            break
        
        level = level + 1
        """ YOUR OUTPUT CODE HERE """
        
    result_list = rdd.collect() #collects the things inside the list 

    for stuff in result_list:
        one = stuff[1]
        two = stuff[0]
        output(str(one) + str(two))
        #output(str(stuff))
    #rdd.coalesce(1).saveAsTextFile(output)

        

    sc.stop()




""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
