pipeline {
    agent any
    environment {
        github = credentials('4145361a-82d9-4899-8224-6e9071be7c45')
    }
    stages {
        stage('Pull') {
            steps {
                git branch: 'master',
                credentialsId: '4145361a-82d9-4899-8224-6e9071be7c45',
                url: 'https://github.com/LightshieldDotDev/RiotApi'
            }
        }
        stage('Select Changes') {
            environment {
            def PROXY_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- proxy || echo changed',
                    returnStdout: true
                ).trim()
            def LEAGUE_UPDATER_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- league_updater || echo changed',
                    returnStdout: true
                ).trim()
            def SUMMONER_ID_UPDATER_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- summoner_id_updater || echo changed',
                    returnStdout: true
                ).trim()
            def MATCH_HISTORY_UPDATER_CHANGED = sh(
                    script: 'git diff --quiet HEAD master -- match_history_updater || echo changed',
                    returnStdout: true
                ).trim()
            }
            steps {
                sh 'echo Ready for rebuilds.'
            }
        }
        stage('Proxy') {
            when {  equals expected: 'changed', actual: env.PROXY_CHANGED}
            steps {
                dir('proxy') {
                    sh 'tox'
                }
            }
        }
        stage('League_Updater') {
            when { equals expected: 'changed', actual: env.LEAGUE_UPDATER_CHANGED}
            steps {
                dir('league_updater') {
                    sh 'tox'
                }
            }
        }
        stage('SummonerID_Updater') {
            when { equals expected: 'changed', actual: env.SUMMONER_ID_UPDATER_CHANGED}
            steps {
                dir('summoner_id_updater') {
                    sh 'tox'
                }
            }
        }
        stage('Match_History_Updater') {
            when { equals expected: 'changed', actual: env.MATCH_HISTORY_UPDATER_CHANGED}
            steps {
                dir('match_history_updater') {
                    sh 'tox'
                }
            }
        }
    }
}