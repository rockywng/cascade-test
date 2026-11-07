package dev.cascade.testengine;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * One MiniCluster for the whole test session.
 *
 * <p>Flink's local executor starts (and tears down) a MiniCluster for every job, which costs most of
 * a second per test. Instead, the engine starts one cluster with enough slots for several tests at
 * once and installs a context environment, so every TableEnvironment created afterwards (on any
 * thread) submits its jobs to that cluster.
 */
final class SharedCluster {

    static final String TARGET = "cascade-shared-minicluster";

    private static MiniCluster cluster;

    private SharedCluster() {}

    static synchronized MiniCluster start(int slots) throws Exception {
        if (cluster != null) {
            return cluster;
        }
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "0");
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, slots);
        // managed memory is a per-slot budget (hash aggregations, RocksDB); Flink's local default
        // of 128m in total is too little once it is split across many slots
        int perSlotMb = Integer.parseInt(System.getProperty("cascade.managed-mb-per-slot", "48"));
        conf.setString("taskmanager.memory.managed.size", (perSlotMb * slots) + "m");
        conf.setString("taskmanager.memory.network.min", "128m");
        conf.setString("taskmanager.memory.network.max", "128m");
        MiniClusterConfiguration mc =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(conf)
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(slots)
                        .setRpcServiceSharing(RpcServiceSharing.SHARED)
                        .build();
        MiniCluster c = new MiniCluster(mc);
        c.start();
        cluster = c;
        ContextEnvironment.install(c);
        return c;
    }

    static synchronized void stop() {
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception ignored) {
                // shutting down anyway
            }
            cluster = null;
        }
    }

    /** Executes pipelines on the shared cluster; the cluster outlives every job. */
    static final class Executor implements PipelineExecutor {
        private final MiniCluster cluster;

        Executor(MiniCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public CompletableFuture<JobClient> execute(
                org.apache.flink.api.dag.Pipeline pipeline, Configuration configuration, ClassLoader cl)
                throws Exception {
            StreamGraph graph = PipelineExecutorUtils.getStreamGraph(pipeline, configuration);
            graph.serializeUserDefinedInstances();
            return cluster.submitJob(graph)
                    .thenApplyAsync(
                            submitted -> {
                                JobID id = submitted.getJobID();
                                try {
                                    ClientUtils.waitUntilJobInitializationFinished(
                                            () -> cluster.getJobStatus(id).get(),
                                            () -> cluster.requestJobResult(id).get(),
                                            cl);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                return (JobClient)
                                        new MiniClusterJobClient(
                                                id,
                                                cluster,
                                                cl,
                                                MiniClusterJobClient.JobFinalizationBehavior.NOTHING);
                            });
        }
    }

    /** Context environment: every StreamExecutionEnvironment created by a TableEnvironment uses it. */
    static final class ContextEnvironment extends StreamExecutionEnvironment {
        private ContextEnvironment() {
            super(new Configuration());
        }

        static void install(MiniCluster cluster) {
            PipelineExecutorFactory factory =
                    new PipelineExecutorFactory() {
                        @Override
                        public String getName() {
                            return TARGET;
                        }

                        @Override
                        public boolean isCompatibleWith(Configuration configuration) {
                            return true;
                        }

                        @Override
                        public PipelineExecutor getExecutor(Configuration configuration) {
                            return new Executor(cluster);
                        }
                    };
            PipelineExecutorServiceLoader loader =
                    new PipelineExecutorServiceLoader() {
                        @Override
                        public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
                            return factory;
                        }

                        @Override
                        public Stream<String> getExecutorNames() {
                            return Stream.of(TARGET);
                        }
                    };
            initializeContextEnvironment(
                    conf -> {
                        Configuration effective = new Configuration();
                        effective.addAll(conf);
                        effective.set(DeploymentOptions.TARGET, TARGET);
                        effective.set(DeploymentOptions.ATTACHED, true);
                        if (!effective.containsKey("parallelism.default")) {
                            effective.setString("parallelism.default", "1");
                        }
                        return new StreamExecutionEnvironment(
                                loader, effective, Thread.currentThread().getContextClassLoader());
                    });
        }
    }
}
