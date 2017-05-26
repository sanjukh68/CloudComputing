Name: Sanju Kurubara Budi Hall Hiriyanna Gowda
UNCC Id: 800953525

Below steps are for executing the code in dsba-cluster.
Step 1: Copy the code file and input files into dsba-cluster
		Eg: pscp <code file> <username>@dsba-hadoop.uncc.edu:/users/<username>/.
			pscp <input file> <username>@dsba-hadoop.uncc.edu:/users/<username>/.
			pscp yxlin2.csv <username>@dsba-hadoop.uncc.edu:/users/<username>/.
Step 2: Login to dsba cluster
		ssh -X <user_name>@dsba-hadoop.uncc.edu
Step 3: Copy the input files into hadoop filesystem
		Eg: hadoop fs -put <input file> /user/<username>/
			hadoop fs -put yxlin2.csv /user/<username>/
Step 4: Execute the code using spark-submit and at the same time copy the output to some file
		Eg: spark-submit <code file> <input file> > output.out
			spark-submit <code file> yxlin2.csv > yxlin2.out
Step 5: Check the output values
		Eg: cat yxlin2.out

		*********OUTPUT*******
		beta:
		<values>
		<values>

To get beta values after convergence. Below steps need to be performed using Gradient descent
Step1 to Step 3 is same for this procedure
Step4: Execute the code using spark-submit and at the same time copy the output to some file
		Eg: spark-submit <code file> <input file> alpha_value iteration_value > (output file)
			spark-submit linreg.py yxlin.csv 0.002 300 > yxlin.out
Step5: Check the contents using cat command.