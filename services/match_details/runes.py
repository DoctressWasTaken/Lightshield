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


def get_trees(name='runesReforged.json'):
    runes = json.loads(open(name, 'r').read())
    keys = {}
    tree_ids = ['P', 'D', 'S', 'I', 'R']
    for tree in runes:
        tree_id = tree_ids[(tree['id'] - 8000) // 100 - 1]
        slots = tree['slots']
        for index, key in enumerate(slots):
            for position, rune in enumerate(key['runes']):
                keys[rune['id']] = tree_id

    return keys


if __name__ == '__main__':
    get_ids()
