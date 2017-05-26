# linreg.py
#
# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# TODO: Write this.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin.csv
#
#
import os
import sys
import numpy as np
from numpy import *

from pyspark import SparkContext
from numpy.linalg import inv

# function to get x points from input file
def getXValues(line):
    values = [float(x) for x in line.split(',')]
    return (values[1:])

# function to get y points from input file
def getYValues(line):
    values = [float(x) for x in line.split(',')]
    return (values[0])

# function to get beta coefficients of linear regression
def linearRegression(xlines,ylines):
    keya = xlines.map(lambda a:("keya",(np.array(a).reshape(len(a),1) * np.transpose(np.array(a).astype('float')).reshape(1,len(a)))))
    A=keya.reduceByKey(lambda x,y : x+y).values()
    keyb = ylines.map(lambda b:("keyb",(np.array(b)[1:len(b)].reshape(len(b)-1,1) * np.array(b)[0].reshape(1,1))))
    B=keyb.reduceByKey(lambda x,y : x+y).values()
    A= np.array(A.collect()).squeeze()
    B= np.array(B.collect())
    aInverse = inv(A)
    coeff = np.dot(aInverse,B)
    return coeff
	
def gradientMatrix(vals):
    biasX = np.array([[1.0]])
    biasedX = append(biasX, vals).astype('float')
    xMatrix = np.transpose(np.asmatrix(biasedX))
    return xMatrix
  
if __name__ == "__main__":
  if len(sys.argv) !=2 and len(sys.argv) !=4:
    print >> sys.stderr, "Usage: linreg <datafile>"
    print "or"
    print "Usage: linreg <datafile> <alpha> <# of iterations>"
    exit(-1)

  sc = SparkContext(appName="linearRegression")

  # Input file which has y(i) as the first element and remaining elements as x(i)'s in each line
  yxinputFile = sc.textFile(sys.argv[1])

  # getting y and x values through getYValues and getXValues functions
  yValues = yxinputFile.map(getYValues).persist()
  xValues = yxinputFile.map(getXValues).persist()

  # creating x and y array using the numpy package 
  x = np.array(xValues.collect()).astype('float')
  y = np.array(yValues.collect()).astype('float')

  # adding a bias term to the x values
  xbias = np.c_[np.ones(len(x)), x]

  xlines = sc.parallelize(xbias)
  ylines = sc.parallelize(np.c_[y,xbias])
  #getting beta coefficients

  if len(sys.argv) == 2:
    #to calculate linear regression beta coefficients
    beta = linearRegression(xlines, ylines)

    # printing the linear regression coefficients in desired output format
    print "beta: "
    for coeff in beta:
      print coeff
  elif len(sys.argv)==4:
    #to calculate gradient descent and beta coefficients
    beta = np.asmatrix(np.zeros(len(xValues.first())+1, dtype=float)).T
    beta.fill(random.uniform(0, 1))
    alpha = float((sys.argv[2]))
    noOfIteration = int((sys.argv[3]))
    keyXvalues = xValues.map(lambda a: gradientMatrix(a))
    keyX = np.asmatrix(np.array(keyXvalues.collect()))
    keyXT = keyX.T
    yVal = np.asmatrix(np.array(yValues.collect())).T.astype('float')

	#looping through iterations to converge beta values
    for i in range(0,noOfIteration):
      xb=np.dot(keyX,beta)
      diff1=np.subtract(yVal,xb)
      diff2 = np.dot(keyXT,diff1)
      alxt = np.dot(alpha,diff2)
      beta = np.add(beta,alxt)

    print "beta: "
    for coeff in beta:
     print coeff

  sc.stop()
