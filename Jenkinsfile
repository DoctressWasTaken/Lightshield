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
            steps {
                sh 'sudo python3.8 -m venv --clear .py3
                    . .py3/bin/activate
                    pip install tox
                    tox'
            }
        }
        stage('Create Network') {
            steps {
                sh 'docker network create lightshield'
            }
        }
        stage('Create Postgres') {
            steps {
                sh 'docker-compose -f compose-persistent.yaml up -d'
            }
        }
        stage('Build Base Image') {
            steps {
                sh 'docker build -t lightshield_service services/base_image/'
            }
        }
        stage('Create NA') {
            environment {
                def SERVER='NA1'
                def COMPOSE_PROJECT_NAME='lightshield_na1'
                }
            steps {
                sh 'docker-compose build'
                sh 'docker-compose up -d'
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
                sh 'docker-compose build'
                sh 'docker-compose up -d'
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
                sh 'docker-compose build'
                sh 'docker-compose up -d'
            }
        }
    }
}
