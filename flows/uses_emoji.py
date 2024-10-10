import emoji
import prefect


@prefect.flow
def emojize():
    return emoji.emojize("Hello, World! :smile:")
