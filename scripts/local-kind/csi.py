"""Generate three single-node instances of the pinned sample CSI driver."""
from copy import deepcopy

import yaml

ZONES = 'abc'
ZONE_LABEL = 'topology.kubernetes.io/zone'
CSI_LIMIT = '10737418240'


def driver_for(zone):
    return f'hostpath-{zone}.live3.csi.k8s.io'


def documents(path):
    return list(yaml.safe_load_all(path.read_text()))


def manifests(directory):
    root = directory / 'csi-hostpath/deploy/kubernetes-latest/hostpath'
    plugin = documents(root / 'csi-hostpath-plugin.yaml')
    driver, = documents(root / 'csi-hostpath-driverinfo.yaml')
    for component in ('attacher', 'provisioner', 'resizer'):
        for item in documents(directory / f'csi-{component}-rbac.yaml'):
            if item['kind'] != 'ServiceAccount':
                yield item
    for item in plugin:
        if item['kind'] != 'StatefulSet' and 'snapshot' not in item['metadata']['name']:
            yield item

    source = next(item for item in plugin if item['kind'] == 'StatefulSet')
    for zone in ZONES:
        name = driver_for(zone)
        definition = deepcopy(driver)
        definition['metadata']['name'] = name
        definition['spec']['attachRequired'] = True
        yield definition
        workload = deepcopy(source)
        workload['metadata']['name'] += '-' + zone
        workload['spec']['serviceName'] = workload['metadata']['name']
        pod = workload['spec']['template']
        pod['metadata']['labels']['live3-zone'] = zone
        workload['spec']['selector']['matchLabels']['live3-zone'] = zone
        pod['spec']['nodeSelector'] = {ZONE_LABEL: zone}
        pod['spec']['containers'] = [container for container in pod['spec']['containers']
                                     if container['name'] != 'csi-snapshotter']
        hostpath = next(container for container in pod['spec']['containers'] if container['name'] == 'hostpath')
        hostpath['image'] = 'registry.k8s.io/sig-storage/hostpathplugin:v1.19.0'
        hostpath['args'] = [f'--drivername={name}' if arg.startswith('--drivername=') else arg
                            for arg in hostpath['args']]
        hostpath['args'].extend(['--enable-attach=true', f'--max-volume-size={CSI_LIMIT}'])
        for container in pod['spec']['containers']:
            container['resources'] = {'requests': {'cpu': '10m', 'memory': '16Mi'},
                                      'limits': {'memory': '128Mi'}}
        yield workload
        yield dict(apiVersion='storage.k8s.io/v1', kind='StorageClass', metadata={'name': f'live3-{zone}'},
                   provisioner=name, volumeBindingMode='WaitForFirstConsumer',
                   allowVolumeExpansion=True, reclaimPolicy='Delete')
