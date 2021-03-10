/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Mounts volumes on the JobManager or TaskManager pod. Volumes can be PVC, empty dir and host path
 * All resources are assumed to exist. Resource definition is contained in FlinkConfiguration as
 * follows: for job manager: volumekey = "kubernetes.jobmanager.volumemount" for task manager:
 * volumekey = "kubernetes.taskmanager.volumemount" Each key can contain multiple volumes separated
 * by ",". The volume itself is defined by parameters separated by ":". Parameters include volume
 * type, mount name, volume path and volume specific parameters
 */
public class VolumeMountDecorator extends AbstractKubernetesStepDecorator {

    public static final String KUBERNETES_VOLUMES_PVC = "pvc";
    public static final String KUBERNETES_VOLUMES_EMPTYDIR = "emptydir";
    public static final String KUBERNETES_VOLUMES_HOSTPATH = "hostpath";
    public static final String EMPTYDIR_DEFAULT = "default";

    private static final Logger LOG = LoggerFactory.getLogger(VolumeMountDecorator.class);

    private final AbstractKubernetesParameters kubernetesComponentConf;
    private List<Volume> volumes = new LinkedList<>();
    private List<VolumeMount> volumeMounts = new LinkedList<>();
    private ConfigOption<String> volumeKey;

    public VolumeMountDecorator(
            AbstractKubernetesParameters kubernetesComponentConf, ConfigOption<String> volumekey) {
        this.volumeKey = volumekey;
        this.kubernetesComponentConf = kubernetesComponentConf;
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {

        // build the list of descriptors
        convertVolumeMountingInfo(
                kubernetesComponentConf.getFlinkConfiguration().getString(volumeKey));
        // Check if we need to add any resources
        if (volumes.size() < 1) {
            return flinkPod;
        }

        // Add volumes to pod
        final Pod podWithMount =
                new PodBuilder(flinkPod.getPod())
                        .editOrNewSpec()
                        .addToVolumes(volumes.toArray(new Volume[0]))
                        .endSpec()
                        .build();

        final Container containerWithMount =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addToVolumeMounts(volumeMounts.toArray(new VolumeMount[0]))
                        .build();

        return new FlinkPod.Builder(flinkPod)
                .withPod(podWithMount)
                .withMainContainer(containerWithMount)
                .build();
    }

    /**
     * Build volume and volume mount for volumes definitions from config string.
     *
     * @param mountInfoString - string representation of volumes
     */
    private void convertVolumeMountingInfo(String mountInfoString) {
        if (mountInfoString == null) {
            return;
        }
        String[] mounts = mountInfoString.split(",");
        for (String mount : mounts) {
            String[] mountInfo = mount.split(":");
            if (mountInfo.length > 0) {
                switch (mountInfo[0]) {
                    case KUBERNETES_VOLUMES_PVC:
                        // pvc
                        if (mountInfo.length >= 5) {
                            List<SubPathToPath> subPaths =
                                    (mountInfo.length == 6)
                                            ? buildSubPaths(mountInfo[5])
                                            : Collections.singletonList(
                                                    new SubPathToPath(null, mountInfo[2]));
                            if (subPaths.size() > 0) {
                                // volume mounts
                                for (SubPathToPath subPath : subPaths) {
                                    volumeMounts.add(
                                            buildVolumeMount(
                                                    mountInfo[1], subPath.path, subPath.subPath));
                                }
                                // volumes
                                volumes.add(
                                        new VolumeBuilder()
                                                .withName(mountInfo[1])
                                                .withNewPersistentVolumeClaim()
                                                .withClaimName(mountInfo[3])
                                                .withNewReadOnly(mountInfo[4])
                                                .endPersistentVolumeClaim()
                                                .build());
                            } else {
                                LOG.error("Failed to process subPath list {}", mountInfo[5]);
                            }
                        } else {
                            LOG.error("Not enough parameters for pvc volume mount {}", mount);
                        }
                        break;
                    case KUBERNETES_VOLUMES_EMPTYDIR:
                        if (mountInfo.length >= 4) {
                            volumeMounts.add(buildVolumeMount(mountInfo[1], mountInfo[2], null));
                            Quantity sizeLimit =
                                    (mountInfo.length == 5) ? new Quantity(mountInfo[4]) : null;
                            String medium =
                                    (mountInfo[3].equals(EMPTYDIR_DEFAULT)) ? "" : mountInfo[3];
                            volumes.add(
                                    new VolumeBuilder()
                                            .withName(mountInfo[1])
                                            .withNewEmptyDir()
                                            .withMedium(medium)
                                            .withSizeLimit(sizeLimit)
                                            .endEmptyDir()
                                            .build());
                        } else {
                            LOG.error("Not enough parameters for emptydir volume mount {}", mount);
                        }
                        break;
                    case KUBERNETES_VOLUMES_HOSTPATH:
                        if (mountInfo.length == 5) {
                            volumeMounts.add(buildVolumeMount(mountInfo[1], mountInfo[2], null));
                            volumes.add(
                                    new VolumeBuilder()
                                            .withName(mountInfo[1])
                                            .withNewHostPath()
                                            .withNewPath(mountInfo[3])
                                            .withNewType(mountInfo[4])
                                            .endHostPath()
                                            .build());
                        } else {
                            LOG.error("Not enough parameters for host path volume mount {}", mount);
                        }
                        break;
                    default:
                        LOG.error("Unsupported volume type {}", mountInfo[0]);
                }
            } else {
                LOG.error("Not enough parameters for volume mount {}", mount);
            }
        }
    }

    /**
     * Build volume.
     *
     * @param name - volume name
     * @param path - volume path
     */
    private VolumeMount buildVolumeMount(String name, String path, String subPath) {
        VolumeMountBuilder builder = new VolumeMountBuilder().withName(name).withMountPath(path);
        if (subPath != null) {
            builder.withSubPath(subPath);
        }
        return builder.build();
    }

    /**
     * Build sub-paths from config string.
     *
     * @param subPathString - string representation items in the format sub-path -> path;....
     */
    private List<SubPathToPath> buildSubPaths(String subPathString) {
        List<SubPathToPath> result = new LinkedList<>();
        String[] items = subPathString.split(";");
        for (String item : items) {
            String[] subPath = item.split("->");
            if (subPath.length == 2) {
                result.add(new SubPathToPath(subPath[0], subPath[1]));
            }
        }
        return result;
    }

    private class SubPathToPath {
        String subPath;
        String path;

        public SubPathToPath(String subPath, String path) {
            this.subPath = subPath;
            this.path = path;
        }
    }
}
