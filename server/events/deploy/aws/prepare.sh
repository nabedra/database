#!/usr/bin/env bash
set -euo pipefail
umask 077

settings=/etc/nabedra-events/deployment.json
config_root=/etc/nabedra-events
mount_dir=/var/lib/nabedra-events
volume_id=$(jq -er '.volume_id' "$settings")
volume_serial=${volume_id/-/}

device=
for (( attempt = 0; attempt < 75; attempt++ )); do
    device=$(lsblk -dnpo NAME,SERIAL,TYPE | awk -v id="$volume_serial" '$2 == id && $3 == "disk" { print $1 }')
    if [[ -b "$device" ]]; then
        break
    fi
    sleep 2
done
[[ -b "$device" ]] || { echo "Expected EBS volume was not attached" >&2; exit 1; }

device_number=$(lsblk -dnro MAJ:MIN "$device")
if mountpoint -q "$mount_dir"; then
    [[ $(findmnt -nro MAJ:MIN --mountpoint "$mount_dir") == "$device_number" ]] \
        || { echo "The Events data directory has the wrong mounted device" >&2; exit 1; }
else
    filesystem=$(lsblk -dnro FSTYPE "$device")
    if [[ -z "$filesystem" ]]; then
        if [[ $(jq -r '.format_blank_volume' "$settings") != true ]] \
            || [[ -n $(wipefs --noheadings --output TYPE "$device") ]] \
            || [[ $(lsblk -nro TYPE "$device") != disk ]]; then
            echo "The expected volume has no supported filesystem and formatting is unavailable" >&2
            exit 1
        fi
        mkfs.xfs "$device" >/dev/null
        filesystem=xfs
    fi
    [[ "$filesystem" == xfs ]] || { echo "The Events volume must use XFS" >&2; exit 1; }
    mount -t xfs -o nodev,nosuid,noexec "$device" "$mount_dir"
fi
chown events:events "$mount_dir"
chmod 0700 "$mount_dir"
if [[ $(jq -r '.format_blank_volume' "$settings") == true ]]; then
    jq '.format_blank_volume = false' "$settings" > "$settings.next"
    mv -Tf "$settings.next" "$settings"
fi

region=$(jq -er '.region' "$settings")
bucket=$(jq -er '.artifact_bucket' "$settings")
key=$(jq -er '.artifact_key' "$settings")
digest=$(jq -er '.artifact_sha256' "$settings")
secret=$(jq -er '.secret_arn' "$settings")

stage=$(mktemp -d "$config_root/config.XXXXXXXX")
finished=false
clean_stage() {
    rm -f "$stage/secret.json" "$stage/artifact" "$config_root/.current-next"
    if [[ "$finished" == false ]]; then
        rm -rf -- "$stage"
    fi
}
trap clean_stage EXIT

artifact=/usr/local/bin/nabedra-events
if [[ ! -x "$artifact" ]] || ! printf '%s  %s\n' "$digest" "$artifact" | sha256sum --status -c -; then
    AWS_PAGER="" aws --region "$region" s3api get-object \
        --bucket "$bucket" --key "$key" "$stage/artifact" >/dev/null
    printf '%s  %s\n' "$digest" "$stage/artifact" | sha256sum --status -c -
    install -m 0755 "$stage/artifact" "$artifact"
fi

AWS_PAGER="" aws --region "$region" secretsmanager get-secret-value \
    --secret-id "$secret" --query SecretString --output text > "$stage/secret.json"
for item in public_cert public_key peer_ca peer_cert peer_key peer_identity; do
    jq -er --arg item "$item" '.[$item] | select(type == "string" and length > 0)' \
        "$stage/secret.json" > "$stage/$item.pem"
done
jq -en --slurpfile settings "$settings" --slurpfile secret "$stage/secret.json" --arg stage "$stage" '
    $settings[0].config as $config |
    $secret[0] as $credentials |
    if ($credentials.peer_token | type) != "string"
        or ($credentials.keyring | type) != "object"
        or ($credentials.keyring.cluster_id != $config.cluster_id)
        or ($credentials.initialize_new_cluster | type) != "boolean"
    then error("invalid Events credentials")
    else $config + {
        peer_token: $credentials.peer_token,
        keyring: $credentials.keyring,
        initialize_new_cluster: $credentials.initialize_new_cluster,
        peer_ca: ($stage + "/peer_ca.pem"),
        peer_identity: ($stage + "/peer_identity.pem"),
        peer_tls: { cert: ($stage + "/peer_cert.pem"), key: ($stage + "/peer_key.pem") },
        public_tls: { cert: ($stage + "/public_cert.pem"), key: ($stage + "/public_key.pem") }
    }
    end
' > "$stage/broker.json"
rm -f "$stage/secret.json" "$stage/artifact"
chgrp events "$stage" "$stage"/*
chmod 0750 "$stage"
chmod 0640 "$stage"/*
previous=$(readlink "$config_root/current" || true)
ln -s "$stage" "$config_root/.current-next"
mv -Tf "$config_root/.current-next" "$config_root/current"
finished=true
case "$previous" in
    "$config_root"/config.*) rm -rf -- "$previous" ;;
esac
