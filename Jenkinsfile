pipeline{
      
    agent { label 'slave_local' }
        
	stages{

            stage('Clean Workspace'){
            steps{
                sh '''
                echo -e "## Limpando o Workspace ##
                   '''
                deleteDir()
            }
        }

        stage('SCM GitHub - Checkout'){
            steps{
                                               
                dir('projeto'){
                    
		 echo 'Pulling...' + env.BRANCH_NAME
        	 checkout scm
			/*sh 'echo -e "## SCM GitHub - Checkout ##"'
                    git branch: 'master',
                    credentialsId: 'd319fe2f-a4b7-4e8c-8b30-2803211f33c4',
                    url: 'https://github.com/wasantos/dlkarp.git', */
			
                    	flow = env.BRANCH_NAME
			switch(flow){  
			case "master":
  			flow = "prd"
 	        	break
   	  		case "development":
    			flow = "qas"
       			break
    			default:
     			flow ="NotFound"
    			}
			println flow
			sh 'echo $flow'
		    }
		}
            }  
         }

        stage('Find directory to build'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## Find directory to build ##"'
                    sh 'pwd'
                    sh 'tree'
                }
            }
        }
        

        stage('Build Dlkarp ARP Scala'){
            steps{
                dir('projeto/arp'){
                    sh 'echo -e "## Build ARP Scala ##"'
                    sh 'pwd'
                    sh 'sbt clean assembly'
                }
            }
        }

         stage('Clean S3'){
            steps{
                dir('projeto/arp/target/scala-2.11'){
                    sh 'aws --version'
                    sh 'aws s3 ls'
                    sh 'pwd'
                    sh 'ls -lrt'
                    sh 'aws s3 rm s3://belcorp-bigdata-functional-dlk-$flow/ARPscala-assembly-0.1.jar'
                }
            }
        }
        
        
        stage('Publisher on S3'){
            steps{
                dir('projeto/arp/target/scala-2.11'){
                    sh 'aws --version'
                    sh 'aws s3 ls'
                    sh 'pwd'
                    sh 'ls -lrt'
                    sh 'aws s3 cp *.jar s3://belcorp-bigdata-functional-dlk-$flow/'
                }
            }
        }    
    }
