from src.log import get_logger


class Tree:

    def __init__(self):
        self.root = Node("/")
        self.logger = get_logger("Tree")

    def put_topic(self, topic: str, value):
        self.logger.info(f"put topic {topic} with value {value}")
        node = self.get_node(topic)
        if node:
            node.set_value(value)
        else:
            raise RuntimeError(f"Error! put_topic(), topic={topic}, value={value}. Check topic syntax!")

    def get_value(self, topic):
        self.logger.info(f"get topic {topic} value")
        node = self.get_node(topic)
        if node:
            return node.get_value()
        else:
            raise RuntimeError(f"Error! get_value(), topic={topic}. Check topic syntax!")

    def subscribe_topic(self, topic, address):
        self.logger.info(f"subscribe {address} on topic {topic}")
        node = self.get_node(topic)
        if node:
            node.add_subscriber(address)
        else:
            raise RuntimeError(f"Error! subscribe_topic(), topic={topic}, address={address}. Check topic syntax!")

    def unsubscribe_topic(self, topic, address):
        node = self.get_node(topic)
        if node:
            node.remove_subscriber(address)
        else:
            raise RuntimeError(f"Error! unsubscribe_topic(), topic={topic}, address={address}. Check topic syntax!")

    def list_subscriptions(self, topic):
        self.logger.info(f"list subscriptions of topic {topic}")
        subscribers = set()

        topic_branches = topic.split("/")
        if topic_branches[0] == '':
            topic_branches = topic_branches[1:]
        if not topic_branches:
            return None

        current_node = self.root
        while len(topic_branches) > 0:
            current_topic = topic_branches[0]
            topic_branches = topic_branches[1:]

            if current_node.contains_child(current_topic):
                current_node = current_node.get_child(current_topic)
            else:
                current_node = current_node.add_child(current_topic)
            subscribers.update(current_node.subscribers)
        return subscribers

    def get_list_topics(self):
        nodes = []
        self.root.get_nodes(nodes)
        nodes.remove(self.root)

        list_topics = []
        for node in nodes:
            # TODO for testing purposes
            # only returns topics with published value
            if node.value is not None:
                list_topics.append(node.get_name())
        return list_topics

    def remove_subscriber(self, address):
        nodes = []
        self.root.get_nodes(nodes)
        nodes.remove(self.root)

        for node in nodes:
            node.remove_subscriber(address)

    def get_node(self, topic):

        topic_branches = topic.split("/")
        if topic_branches[0] == '':
            topic_branches = topic_branches[1:]
        if not topic_branches:
            return None

        current_node = self.root
        while len(topic_branches) > 0:
            current_topic = topic_branches[0]
            topic_branches = topic_branches[1:]

            if current_node.contains_child(current_topic):
                current_node = current_node.get_child(current_topic)
            else:
                current_node = current_node.add_child(current_topic)
        return current_node


class Node:

    def __init__(self, topic, value=None, parent=None):
        self.topic = topic
        self.value = value
        self.parent = parent
        self.subscribers = set()
        self.children = list()
        self.logger = get_logger(f"Node {self.get_name()}")

    def contains_child(self, topic):
        for child in self.children:
            if child.topic == topic:
                return True
        return False

    def get_child(self, topic):
        for child in self.children:
            if child.topic == topic:
                return child
        return None

    def add_child(self, topic):
        child = Node(topic, parent=self)
        self.children.append(child)
        return child

    def get_name(self, list_names=None):
        if list_names is None:
            list_names = []
        list_names.append(self.topic)
        if self.parent is None:
            list_names = list_names[::-1]
            name = "/" + "/".join(list_names[1:])
            return name
        return self.parent.get_name(list_names=list_names)

    def get_nodes(self, nodes):
        nodes.append(self)
        for child in self.children:
            child.get_nodes(nodes)

    def set_value(self, value):
        self.value = value

    def get_value(self):
        return self.value

    def add_subscriber(self, address):
        self.logger.info(f"subscribe sock {address}")
        self.subscribers.add(address)

    def remove_subscriber(self, address):
        self.logger.info(f"unsubscribe sock {address}")
        self.subscribers.discard(address)
