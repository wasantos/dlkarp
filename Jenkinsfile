pipeline{
    agent none
        stages{     
            stage('Clean Workspace'){
            steps{
                sh 'echo -e "## Limpando o Workspace ##"'
                deleteDir()
            }
        }

        stage('SCM - GitHub'){
            steps{
                dir('projeto'){
                    sh 'echo -e "## Innersource Checkout ##"'
                    git branch: 'master',
                    credentialsId: '9a54ae94-57c6-46ae-9ce0-4974a758182d',
                    url: 'https://github.com/wasantos/dlkarp.git'
                }
            }  
        }

        stage('Build Dlkarp ARP Scala '){
            steps{
                dir('arp'){
                    sh 'echo -e "## Build ARP Scala ##"'
                    sh 'sbt clean assembly'
                }
            }
        }
    }
}
