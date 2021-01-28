pipeline {
    agent any
    environment {
        github = credentials('40e37eb6-34d9-4bc0-9d2d-d924c671ee85')
        url = 'github.com/LightshieldDotDev/Lightshield'
    }
    stages {
        stage('Fetch git') {
            steps {
               git branch: 'master',
                credentialsId: '40e37eb6-34d9-4bc0-9d2d-d924c671ee85',
                url: 'https://' + env.url
            }
        }
        stage('Tox') {
            when {
                expression { false }
            }
            steps {
                sh 'sudo python3.8 -m venv --clear .py3'
                sh '. .py3/bin/activate'
                sh 'pip install tox'
                sh 'tox'
            }
        }
        stage('Create Network') {
            steps {
                sh 'sudo docker network create lightshield || true'
            }
        }
        stage('Create Postgres') {
            steps {
                sh 'sudo docker-compose -f compose-persistent.yaml up -d'
            }
        }
        stage('Build Base Image') {
            steps {
                sh 'sudo docker build -t lightshield_service services/base_image/'
            }
        }
        stage('Create NA') {
            environment {
                def SERVER='NA1'
                def COMPOSE_PROJECT_NAME='lightshield_na1'
                }
            steps {
                sh 'sudo docker-compose build'
                sh 'sudo docker-compose up -d'
            }
        }
        stage('Create EUW') {
            when {
                expression { false }
            }
            environment {
                def SERVER='EUW1'
                def COMPOSE_PROJECT_NAME='lightshield_euw1'
                }
            steps {
                sh 'sudo docker-compose build'
                sh 'sudo docker-compose up -d'
            }
        }
        stage('Create KR') {
            when {
                expression { false }
            }
            environment {
                def SERVER='KR'
                def COMPOSE_PROJECT_NAME='lightshield_kr'
                }
            steps {
                sh 'sudo docker-compose build'
                sh 'sudo docker-compose up -d'
            }
        }
    }
}
