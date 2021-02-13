import json


def get_ids(name='runesReforged.json'):
    runes = json.loads(open(name, 'r').read())
    keys = {}
    for tree in runes:
        slots = tree['slots']
        for index, key in enumerate(slots):
            modifier = pow(10, 3 - index)
            for position, rune in enumerate(key['runes']):
                keys[rune['id']] = (position + 1) * modifier

    return keys


if __name__ == '__main__':
    get_ids()
