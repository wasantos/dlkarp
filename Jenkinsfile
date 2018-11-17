pipeline{
    agent { label 'slave_local' }
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "## Limpando o Workspace ##"'
                deleteDir()
            }
        }

        stage('SCM GitHub - Checkout'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## SCM GitHub - Checkout ##"'
                    git branch: 'master',
                    credentialsId: 'd319fe2f-a4b7-4e8c-8b30-2803211f33c4',
                    url: 'https://github.com/wasantos/dlkarp.git'
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
    }
}
