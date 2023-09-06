package io.steve;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

@SpringBootApplication
public class PollerJobSpringbootApplication {

	private static final String FILE_PATH = "/var/tmp/";

	public static void main(String[] args) throws Exception {
		SpringApplication.run(PollerJobSpringbootApplication.class, args);

		Path path = Paths.get(FILE_PATH);
		WatchService watcher = FileSystems.getDefault().newWatchService();
		path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE);

		try {
			while (true) {
				// Get directory changes:
				// take() is a blocking method that waits for a signal from the monitor before
				// returning.
				// You can also use the watcher.poll() method, a non-blocking method that will
				// immediately return
				// whether there is a signal in the watcher at that time.
				// The returned result, WatchKey, is a singleton object,
				// which is the same as the instance returned by the previous register method.
				WatchKey key = watcher.take();
				// Handling file change events：
				// key.pollEvents()It is used to obtain file change events,
				// which can only be obtained once and cannot be obtained repeatedly,
				// similar to the form of a queue.
				for (WatchEvent<?> event : key.pollEvents()) {
					// event.kind()：event type
					if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
						// event may be lost or discarded
						continue;
					}
					// Returns the path (relative path) of the file or directory that triggered the
					// event
					Path fileName = (Path) event.context();

					if (!fileName.endsWith(".processing") && !fileName.endsWith(".done")) {
						System.out.println("file changed: " + fileName);

						try (OpenShiftClient client = new KubernetesClientBuilder().build()
								.adapt(OpenShiftClient.class)) {
							Job job = new JobBuilder()
									.withNewMetadata()
									.withName("file-processor-" + fileName.getFileName())
									.endMetadata()
									.withNewSpec()
									.withNewTemplate()
									.withNewSpec()
									.addNewContainer()
									.withName("sleep-container")
									.withImage("alpine")
									.withCommand("sleep", "30")
									.addNewVolumeMount()
									.withMountPath("/test")
									.withName("test-volume")
									.endVolumeMount()
									.endContainer()
									.withVolumes()
									.addNewVolume()
									.withName("test-volume")
									.withEmptyDir(new EmptyDirVolumeSourceBuilder().build())
									.endVolume()
									.withRestartPolicy("Never")
									.endSpec()
									.endTemplate()
									.endSpec()
									.build();

							ConfigMap cm = client.configMaps().inNamespace(client.getNamespace())
									.withName("test-configmap").get();

							if (cm != null) {
								List<EnvVar> envs = new ArrayList<>();
								for (Map.Entry<String, String> entry : cm.getData().entrySet()) {
									EnvVar ev = new EnvVar();
									ev.setName(entry.getKey());
									ev.setValue(entry.getValue());
									envs.add(ev);
									System.out.println("Adding ENV : " + ev.toString());
								}
								job.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envs);
							} else {
								System.out.println("Config map not found");
							}

							client.batch().v1().jobs().inNamespace(client.getNamespace()).resource(job).create();

						} catch (Exception e) {
							System.err.println("Error creating Kubernetes Job: " + e.getMessage());
							e.printStackTrace();
						}

					}
				}
				// This method needs to be reset every time the take() or poll() method of
				// WatchService is called
				if (!key.reset()) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
