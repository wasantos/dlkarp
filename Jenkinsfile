pipeline{
      
    agent { label 'slave_local'}
       stages{
	 stage('Clean Workspace'){
            steps{
                sh '''
                echo -e "## Limpando o Workspace ###"
                   '''
		deleteDir()
            }
        }

        stage('SCM GitHub - Checkout'){
            steps{
                                               
                dir('projeto'){
		               
		echo 'Pulling...' + env.BRANCH_NAME
		checkout scm
		
		sh '''
		   echo ${BRANCH_NAME} "origem"		
  		   case ${BRANCH_NAME} in
			master)     	 FLOW="prd"       ;;
	   		development)     FLOW="qas"       ;;
	    		*)           	 FLOW="default"   ;;
		   esac
    			echo ${FLOW} > flow.tmp
    			cat flow.tmp
		   '''
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
                sh '''
		flow=`cat ../../../flow.tmp`
		pwd
		ls -lrt
 		aws --version
                aws s3 ls
                aws s3 rm s3://belcorp-bigdata-functional-dlk-$flow/ARPscala-assembly-0.1.jar
		'''
                }
            }
        }
        
        
        stage('Publisher on S3'){
            steps{
                dir('projeto/arp/target/scala-2.11'){
		    sh '''
		    flow=`cat ../../../flow.tmp`
                    aws --version
                    aws s3 ls
                    pwd
                    ls -lrt
                    aws s3 cp *.jar s3://belcorp-bigdata-functional-dlk-$flow/
		    '''
           }
        }
     }
  }
}
