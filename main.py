import dtlpy as dl

if dl.token_expired():
    dl.login()

project = dl.projects.get(project_name='FaaS Assignment')
source = project.datasets.get(dataset_id='63238af65ac631a5ca230b64')


def copyNewItem(item: dl.Item):
    project = dl.projects.get(project_name='FaaS Assignment')
    target = project.datasets.get(dataset_id='63238afe5ac6310260230b66')
    copy_annotations = True
    flat_copy = False
    # Download item (without save to disk)
    buffer = item.download(save_locally=False)
    # Give the item's name to the buffer
    if flat_copy:
        buffer.name = item.name
    else:
        buffer.name = item.filename[len('source') + 1:]
    # Upload item
    print("Going to add {} to {} dir".format(buffer.name, target))
    new_item = target.items.upload(local_path=buffer)
    if not isinstance(new_item, dl.Item):
        print('The file {} could not be uploaded'.format(buffer.name))
    print("{} has been uploaded".format(new_item.filename))
    if copy_annotations:
        new_item.annotations.upload(item.annotations.list())

service = project.services.deploy(func=copyNewItem, service_name='copy-new-item-2-target')

filters = dl.Filters()
filters.add(field='datasetId', values=source.id)
trigger = service.triggers.create(name='copy-item-2-target',
                                  function_name='copyNewItem',
                                  execution_mode=dl.TriggerExecutionMode.ONCE,
                                  resource=dl.TriggerResource.ITEM,
                                  actions=dl.TriggerAction.CREATED,
                                  filters=filters)