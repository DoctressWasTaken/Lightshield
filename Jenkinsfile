pipeline {
    agent any
    environment {
        github = credentials('4145361a-82d9-4899-8224-6e9071be7c45')
        url = 'github.com/LightshieldDotDev/RiotApi'
    }
    stages {
        stage('Fetch git') {
            steps {
                script {
                    def BEFORE = sh(
                        script: 'git log --format="%H" -n 1',
                        returnStdout: true
                    ).trim()
                    env.BEFORE = BEFORE
                }
               git branch: 'master',
                credentialsId: '4145361a-82d9-4899-8224-6e9071be7c45',
                url: 'https://' + env.url
                script {
                    def AFTER = sh(
                        script: 'git log --format="%H" -n 1',
                        returnStdout: true
                    ).trim()
                    env.AFTER = AFTER
                }
                echo "FROM ${env.BEFORE} to ${env.AFTER}"
            }
        }
        stage('Set Changes and Pull') {
            environment {
            def PROXY_CHANGED = sh(
                    script: 'git diff master...origin/master -- proxy || echo changed',
                    returnStdout: true
                ).trim()
            def LEAGUE_UPDATER_CHANGED = sh(
                    script: 'git diff master...origin/master -- league_updater || echo changed',
                    returnStdout: true
                ).trim()
            def SUMMONER_ID_UPDATER_CHANGED = sh(
                    script: 'git diff master...origin/master -- summoner_id_updater || echo changed',
                    returnStdout: true
                ).trim()
            def MATCH_HISTORY_UPDATER_CHANGED = sh(
                    script: 'git diff master...origin/master -- match_history_updater || echo changed',
                    returnStdout: true
                ).trim()
            }
            steps {
                git branch: 'master',
                credentialsId: '4145361a-82d9-4899-8224-6e9071be7c45',
                url: 'https://' + env.url
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