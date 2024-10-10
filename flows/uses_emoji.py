import emoji
import prefect


@prefect.flow(log_prints=True)
def emojize():
    result = emoji.emojize("Hello, World! :smile:")
    print(result)
    return result
