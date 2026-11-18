"""Optional host-local maintenance for verified whole-device EBS filesystems."""
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

from ..providers import ProviderError
from .docker import identity


class EbsLinuxHost:
    """Run on the configured Nitro host, in the host PID and mount namespaces.

    Only preconfigured mount paths can be grown. The host operator unmounts
    closed filesystems before detach and serializes any external mount changes.
    """

    def __init__(self, *, instance_id, mount_paths):
        if not isinstance(instance_id, str) or not re.fullmatch(r'i-(?:[a-f0-9]{8}|[a-f0-9]{17})', instance_id):
            raise ProviderError('Exact local EC2 instance ID is required')
        self.instance_id = instance_id
        self.mount_paths = frozenset(mount_paths)
        if (not 0 < len(self.mount_paths) <= 64
                or any(not isinstance(path, str) or path == '/' or not Path(path).is_absolute()
                       or str(Path(path)) != path or '..' in Path(path).parts for path in self.mount_paths)):
            raise ProviderError('Explicit EBS host maintenance mount paths are required')

    def _require_host(self, instance_id, deadline):
        if instance_id != self.instance_id or sys.platform != 'linux' or os.geteuid() != 0 or time.monotonic() >= deadline:
            raise ProviderError('EBS maintenance requires the configured root Linux host and a live deadline')
        try:
            uid = Path('/sys/class/dmi/id/board_asset_tag').read_text().strip()
            for ns in ('mnt', 'pid'):
                if os.readlink('/proc/1/ns/' + ns) != os.readlink('/proc/self/ns/' + ns):
                    raise ValueError
            if uid != instance_id:
                raise ValueError
        except (OSError, ValueError):
            raise ProviderError('EBS maintenance is not on the expected Nitro instance') from None

    def _device(self, expected):
        path = expected['mount_path']
        if path not in self.mount_paths:
            raise ProviderError('EBS mount path is outside host maintenance configuration')
        try:
            link = Path('/dev/disk/by-uuid') / identity(expected['filesystem_uuid'])
            block = link.stat()
            if not stat.S_ISBLK(block.st_mode):
                raise ValueError
            device = (os.major(block.st_rdev), os.minor(block.st_rdev))
            sysfs = Path(f'/sys/dev/block/{device[0]}:{device[1]}').resolve(strict=True)
            serial = (sysfs / 'device/serial').read_text().strip()
            model = (sysfs / 'device/model').read_text().strip()
            if ((sysfs / 'partition').exists() or serial != expected['provider_volume_id'].replace('-', '')
                    or model != 'Amazon Elastic Block Store'):
                raise ValueError
            return link, device
        except (OSError, ValueError):
            raise ProviderError('EBS maintenance volume and whole-device filesystem UUID differ') from None

    def grow(self, instance_id, expected, *, deadline):
        self._require_host(instance_id, deadline)
        link, device = self._device(expected)
        path = expected['mount_path']
        try:
            # The configured host owns the mount table. Check the device and
            # whole filesystem root again immediately before invoking the grow tool.
            if str(Path(path).resolve(strict=True)) != path or Path(path).stat().st_dev != os.makedev(*device):
                raise ValueError
            encoded = path.replace('\\', r'\134').replace(' ', r'\040').replace('\t', r'\011')
            fields = [line.split() for line in Path('/proc/self/mountinfo').read_text().splitlines()]
            mounted = [line for line in fields if line[4] == encoded]
            if len(mounted) != 1:
                raise ValueError
            line = mounted[0]
            if line[2] != f'{device[0]}:{device[1]}' or line[3] != '/' or 'rw' not in line[5].split(','):
                raise ValueError
            fs_type = line[line.index('-') + 1]
            command = {'xfs': ['/usr/sbin/xfs_growfs', path],
                       'ext4': ['/usr/sbin/resize2fs', str(link)]}[fs_type]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError
            subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=True, timeout=remaining)
        except (KeyError, OSError, ValueError, subprocess.SubprocessError):
            raise ProviderError('EBS host filesystem growth is unresolved') from None

    def unmounted(self, instance_id, mount, *, deadline):
        self._require_host(instance_id, deadline)
        _, device = self._device(mount['expected'])
        if device != (mount['device_major'], mount['device_minor']):
            raise ProviderError('Closed EBS mount device has changed')
        device_field = f'{device[0]}:{device[1]}'
        try:
            namespaces = set()
            for index, process in enumerate(Path('/proc').iterdir()):
                if index >= 32768 or time.monotonic() >= deadline:
                    raise ProviderError('EBS mount inventory exceeded the maintenance bound')
                if not process.name.isdecimal():
                    continue
                try:
                    namespace = os.readlink(process / 'ns/mnt')
                    if namespace in namespaces:
                        continue
                    with (process / 'mountinfo').open() as table:
                        if any(line.split(maxsplit=3)[2] == device_field for line in table):
                            return False
                    namespaces.add(namespace)
                except (FileNotFoundError, ProcessLookupError):
                    continue
        except OSError:
            raise ProviderError('EBS mount inventory could not verify unmount') from None
        return True
