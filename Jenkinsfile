pipeline{
    agent { label 'slave-local' }
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "\033[0;34m## Limpando o Workspace ##\033[0m"'
                deleteDir()
            }
        }

        stage('SCM - GitHub'){
            steps{
                dir('projeto'){
                    sh 'echo -e "\033[0;34m ## Innersource Checkout ##\033[0m"'
                    git branch: 'master',
                    credentialsId: '9a54ae94-57c6-46ae-9ce0-4974a758182d',
                    url: 'https://github.com/wasantos/DataLake.git'
                }
            }  
        }

        stage('Build Datalake'){
            steps{
                dir('projeto'){
                    sh 'echo -e "\033[0;34m ## Build Datalake ##\033[0m"'
                    sh 'source /home/centos/.bashrc'
                    sh 'cd datalake'
                    sh 'pwd'
                    sh '/usr/bin/python3.6 --version'
                    sh '/usr/bin/python3.6 build.py'
                }
            }
        }
    }
}