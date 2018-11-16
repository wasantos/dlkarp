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
                    sh 'echo -e "## Checkout ##"'
                    git branch: 'master',
                    credentialsId: 'd319fe2f-a4b7-4e8c-8b30-2803211f33c4',
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
