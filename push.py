import sys

import docker


def main():
    repository = "lightshield.dev:5000/"
    try:
        version = sys.argv[1]
    except:
        version = 'latest'

    client = docker.from_env()

    for image in client.images.list():
        for tag in image.tags:
            if tag.startswith('lightshield_'):
                image.tag(repository + tag, tag=version)
                client.images.push(repository + tag, tag=version)
                continue


if __name__ == "__main__":
    main()
