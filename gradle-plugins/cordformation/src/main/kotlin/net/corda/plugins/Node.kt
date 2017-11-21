package net.corda.plugins

import com.typesafe.config.*
import groovy.lang.Closure
import net.corda.cordform.CordformNode
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.RDN
import org.bouncycastle.asn1.x500.style.BCStyle
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.model.ObjectFactory
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import javax.inject.Inject

/**
 * Represents a node that will be installed.
 */
open class Node @Inject constructor(private val project: Project, private val objectFactory: ObjectFactory) : CordformNode() {
    companion object {
        @JvmStatic
        val nodeJarName = "corda.jar"
        @JvmStatic
        val webJarName = "corda-webserver.jar"
        private val configFileProperty = "configFile"
        val capsuleCacheDir: String = "./cache"
    }

    fun fullPath(): Path = project.projectDir.toPath().resolve(nodeDir.toPath())
    fun logDirectory(): Path = fullPath().resolve("logs")
    fun makeLogDirectory() = Files.createDirectories(logDirectory())

    /**
     * Set the list of CorDapps to install to the plugins directory. Each cordapp is a fully qualified Maven
     * dependency name, eg: com.example:product-name:0.1
     *
     * @note Your app will be installed by default and does not need to be included here.
     * @note Type is any due to gradle's use of "GStrings" - each value will have "toString" called on it
     */
    @Deprecated("Use cordapp instead - will be removed by Corda V4.0")
    var cordapps: MutableList<Any> = mutableListOf<Any>()
        set(value) {
            value.forEach {
                cordapp({
                    coordinates = it.toString()
                })
            }
        }

    private val internalCordapps = mutableListOf<Cordapp>()
    private val releaseVersion = project.rootProject.ext<String>("corda_release_version")
    internal lateinit var nodeDir: File

    /**
     * Sets whether this node will use HTTPS communication.
     *
     * @param isHttps True if this node uses HTTPS communication.
     */
    fun https(isHttps: Boolean) {
        config = config.withValue("useHTTPS", ConfigValueFactory.fromAnyRef(isHttps))
    }

    /**
     * Sets the H2 port for this node
     */
    fun h2Port(h2Port: Int) {
        config = config.withValue("h2port", ConfigValueFactory.fromAnyRef(h2Port))
    }

    fun useTestClock(useTestClock: Boolean) {
        config = config.withValue("useTestClock", ConfigValueFactory.fromAnyRef(useTestClock))
    }

    /**
     * Set the HTTP web server port for this node. Will use localhost as the address.
     *
     * @param webPort The web port number for this node.
     */
    fun webPort(webPort: Int) {
        config = config.withValue("webAddress",
                ConfigValueFactory.fromAnyRef("$DEFAULT_HOST:$webPort"))
    }

    /**
     * Set the HTTP web server address and port for this node.
     *
     * @param webAddress The web address for this node.
     */
    fun webAddress(webAddress: String) {
        config = config.withValue("webAddress",
                ConfigValueFactory.fromAnyRef(webAddress))
    }

    /**
     * Set the network map address for this node.
     *
     * @warning This should not be directly set unless you know what you are doing. Use the networkMapName in the
     *          Cordform task instead.
     * @param networkMapAddress Network map node address.
     * @param networkMapLegalName Network map node legal name.
     */
    fun networkMapAddress(networkMapAddress: String, networkMapLegalName: String) {
        val networkMapService = mutableMapOf<String, String>()
        networkMapService.put("address", networkMapAddress)
        networkMapService.put("legalName", networkMapLegalName)
        config = config.withValue("networkMapService", ConfigValueFactory.fromMap(networkMapService))
    }

    /**
     * Set the SSHD port for this node.
     *
     * @param sshdPort The SSHD port.
     */
    fun sshdPort(sshdPort: Int) {
        config = config.withValue("sshdAddress",
                ConfigValueFactory.fromAnyRef("$DEFAULT_HOST:$sshdPort"))
    }

    /**
     * Add a cordapp to this node
     *
     * @param configureClosure A groovy closure to configure a [Cordapp] object
     * @return The created and inserted [Cordapp]
     */
    fun cordapp(configureClosure: Closure<in Cordapp>): Cordapp {
        val cordapp = project.configure(objectFactory.newInstance(Cordapp::class.java), configureClosure) as Cordapp
        addCordapp(cordapp)
        return cordapp
    }

    /**
     * Add a cordapp to this node
     *
     * @param configureFunc A lambda to configure a [Cordapp] object
     * @return The created and inserted [Cordapp]
     */
    fun cordapp(configureFunc: Cordapp.() -> Unit): Cordapp {
        val cordapp = objectFactory.newInstance(Cordapp::class.java).apply { configureFunc() }
        addCordapp(cordapp)
        return cordapp
    }

    internal fun build() {
        configureProperties()
        installCordaJar()
        if (config.hasPath("webAddress")) {
            installWebserverJar()
        }
        installBuiltCordapp()
        installCordapps()
        installConfig()
        appendOptionalConfig()
    }

    internal fun rootDir(rootDir: Path) {
        if(name == null) {
            project.logger.error("Node has a null name - cannot create node")
            throw IllegalStateException("Node has a null name - cannot create node")
        }

        val dirName = try {
            val o = X500Name(name).getRDNs(BCStyle.O)
            if (o.size > 0) {
                o.first().first.value.toString()
            } else {
                name
            }
        } catch(_ : IllegalArgumentException) {
            // Can't parse as an X500 name, use the full string
            name
        }
        nodeDir = File(rootDir.toFile(), dirName)
    }

    private fun configureProperties() {
        config = config.withValue("rpcUsers", ConfigValueFactory.fromIterable(rpcUsers))
        if (notary != null) {
            config = config.withValue("notary", ConfigValueFactory.fromMap(notary))
        }
        if (extraConfig != null) {
            config = config.withFallback(ConfigFactory.parseMap(extraConfig))
        }
    }

    /**
     * Installs the corda fat JAR to the node directory.
     */
    private fun installCordaJar() {
        val cordaJar = verifyAndGetCordaJar()
        project.copy {
            it.apply {
                from(cordaJar)
                into(nodeDir)
                rename(cordaJar.name, nodeJarName)
                fileMode = Cordformation.executableFileMode
            }
        }
    }

    /**
     * Installs the corda webserver JAR to the node directory
     */
    private fun installWebserverJar() {
        val webJar = verifyAndGetWebserverJar()
        project.copy {
            it.apply {
                from(webJar)
                into(nodeDir)
                rename(webJar.name, webJarName)
            }
        }
    }

    /**
     * Installs this project's cordapp to this directory.
     */
    private fun installBuiltCordapp() {
        val cordappsDir = File(nodeDir, "cordapps")
        project.copy {
            it.apply {
                from(project.tasks.getByName("jar"))
                into(cordappsDir)
            }
        }
    }

    /**
     * Installs other cordapps to this node's cordapps directory.
     */
    private fun installCordapps() {
        val cordappsDir = File(nodeDir, "cordapps")
        val cordapps = getCordappList()
        project.copy {
            it.apply {
                from(cordapps)
                into(cordappsDir)
            }
        }
    }

    /**
     * Installs the configuration file to this node's directory and detokenises it.
     */
    private fun installConfig() {
        val options = ConfigRenderOptions
                .defaults()
                .setOriginComments(false)
                .setComments(false)
                .setFormatted(true)
                .setJson(false)
        val configFileText = config.root().render(options).split("\n").toList()

        // Need to write a temporary file first to use the project.copy, which resolves directories correctly.
        val tmpDir = File(project.buildDir, "tmp")
        tmpDir.mkdir()
        val tmpConfFile = File(tmpDir, "node.conf")
        Files.write(tmpConfFile.toPath(), configFileText, StandardCharsets.UTF_8)

        project.copy {
            it.apply {
                from(tmpConfFile)
                into(nodeDir)
            }
        }
    }

    /**
     * Appends installed config file with properties from an optional file.
     */
    private fun appendOptionalConfig() {
        val optionalConfig: File? = when {
            project.findProperty(configFileProperty) != null -> //provided by -PconfigFile command line property when running Gradle task
                File(project.findProperty(configFileProperty) as String)
            config.hasPath(configFileProperty) -> File(config.getString(configFileProperty))
            else -> null
        }

        if (optionalConfig != null) {
            if (!optionalConfig.exists()) {
                project.logger.error("$configFileProperty '$optionalConfig' not found")
            } else {
                val confFile = File(project.buildDir.path + "/../" + nodeDir, "node.conf")
                confFile.appendBytes(optionalConfig.readBytes())
            }
        }
    }

    /**
     * Find the corda JAR amongst the dependencies.
     *
     * @return A file representing the Corda JAR.
     */
    private fun verifyAndGetCordaJar(): File {
        val maybeCordaJAR = project.configuration("runtime").filter {
            it.toString().contains("corda-$releaseVersion.jar") || it.toString().contains("corda-enterprise-$releaseVersion.jar")
        }
        if (maybeCordaJAR.isEmpty) {
            throw RuntimeException("No Corda Capsule JAR found. Have you deployed the Corda project to Maven? Looked for \"corda-$releaseVersion.jar\"")
        } else {
            val cordaJar = maybeCordaJAR.singleFile
            assert(cordaJar.isFile)
            return cordaJar
        }
    }

    /**
     * Find the corda JAR amongst the dependencies
     *
     * @return A file representing the Corda webserver JAR
     */
    private fun verifyAndGetWebserverJar(): File {
        val maybeJar = project.configuration("runtime").filter {
            it.toString().contains("corda-webserver-$releaseVersion.jar")
        }
        if (maybeJar.isEmpty) {
            throw RuntimeException("No Corda Webserver JAR found. Have you deployed the Corda project to Maven? Looked for \"corda-webserver-$releaseVersion.jar\"")
        } else {
            val jar = maybeJar.singleFile
            assert(jar.isFile)
            return jar
        }
    }

    /**
     * Gets a list of cordapps based on what dependent cordapps were specified.
     *
     * @return List of this node's cordapps.
     */
    private fun getCordappList(): Collection<File> {
        val cordappConfiguration = project.configuration("cordapp")
        // Cordapps can sometimes contain a GString instance which fails the equality test with the Java string
        @Suppress("RemoveRedundantCallsOfConversionMethods")
        val cordapps: List<String> = cordapps.map { it.toString() } + internalCordapps.map { it.coordinates!! }
        return cordapps.map { cordappName ->
            val cordappFile = cordappConfiguration.files { cordappName == (it.group + ":" + it.name + ":" + it.version) }

            when {
                cordappFile.size == 0 -> throw GradleException("Cordapp $cordappName not found in cordapps configuration.")
                cordappFile.size > 1 -> throw GradleException("Multiple files found for $cordappName")
                else -> cordappFile.single()
            }
        }
    }

    private fun addCordapp(cordapp: Cordapp) {
        // TODO: Use gradle @Input annotation to make this required in the build script
        if(cordapp.coordinates == null) {
            throw GradleException("cordapp is missing coordinates field")
        }
        internalCordapps += cordapp
    }
}