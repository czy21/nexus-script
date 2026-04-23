import groovy.json.JsonOutput

import java.math.RoundingMode
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.PathMatcher

import static groovy.io.FileType.FILES

REPOSITORY_WHITELIST = []

REPOSITORY_BLACKLIST = []

Map<String, File> blobStoreDirectories = [:]
hasWhitelist = REPOSITORY_WHITELIST.size() > 0
hasBlacklist = !hasWhitelist && REPOSITORY_BLACKLIST.size() > 0

String SEP = FileSystems.getDefault().getSeparator()
if ('\\' == SEP) {
    SEP = "${SEP}${SEP}"  // escape back slashes on windows so path matchers work correctly
    log.info("Treating file system as using Windows path separators.")
}

def EXCLUDE_PATTERNS = "glob:{" +
        "**${SEP}metadata.properties," +
        "**${SEP}*metrics.properties," +
        "**${SEP}*.bytes," +
        "**${SEP}tmp*," +
        "**${SEP}*deletions.index," +
        "**${SEP}*.DS_Store}"
log.info("Global Blobstore exclude patterns: {}", EXCLUDE_PATTERNS)
PathMatcher EXCLUDE_MATCHER = FileSystems.getDefault().getPathMatcher(EXCLUDE_PATTERNS)

Map<String, BlobStatistics> blobStatCollection = [:].withDefault { 0 }

class BlobStatistics {
    int totalRepoNameMissingCount = 0
    long totalBlobStoreBytes = 0
    BigDecimal totalBlobStoreGB = 0
    long totalReclaimableBytes = 0
    BigDecimal totalReclaimableGB = 0
    Map<String, RepoStatistics> repositories = [:]
}

class RepoStatistics {
    long totalBytes = 0
    BigDecimal totalGB = 0
    long reclaimableBytes = 0
    BigDecimal reclaimableGB = 0
}

def collectMetrics(final BlobStatistics blobstat, Set<String> unmapped, final Properties properties, final File propertiesFile) {
    def repo = properties.'@Bucket.repo-name'
    if (repo == null && properties.'@BlobStore.direct-path') {
        repo = 'SYSTEM:direct-path'
    }
    if (repo == null) {
        // unexpected - log the unexpected condition
        if (blobstat.totalRepoNameMissingCount <= 50) {
            log.warn('Repository name missing from {} : {}', propertiesFile.absolutePath, properties)
            log.info('full details: {}', properties)
        }
        blobstat.totalRepoNameMissingCount++
    } else {
        if (!blobstat.repositories.containsKey(repo)) {
            if (!unmapped.contains(repo)) {
                if (!repo.equals('SYSTEM:direct-path')) {
                    log.info('Found unknown repository in {}: {}', propertiesFile.absolutePath, repo)
                }
                blobstat.repositories.put(repo as String, new RepoStatistics())
            }
        }

        if (blobstat.repositories.containsKey(repo)) {
            blobstat.repositories."$repo".totalBytes += (properties.size as long)
            if (!repo.equals('SYSTEM:direct-path')) {
                blobstat.totalBlobStoreBytes += (properties.size as long)
            }

            if (properties.'deleted') {
                blobstat.repositories."$repo".reclaimableBytes += (properties.size as long)
                if (!repo.equals('SYSTEM:direct-path')) {
                    blobstat.totalReclaimableBytes += (properties.size as long)
                }
            }
        }
    }
}

def passesWhiteBlackList(final String name) {
    if (hasWhitelist) {
        return REPOSITORY_WHITELIST.contains(name)
    }
    if (hasBlacklist) {
        return !REPOSITORY_BLACKLIST.contains(name)
    }
    return true
}

Map<String, Map<String, Boolean>> storeRepositoryLookup = [:].withDefault { [:] }

repository.repositoryManager.browse().each { repo ->
    def blobStoreName = repo.properties.configuration.attributes.storage.blobStoreName
    storeRepositoryLookup.get(blobStoreName).put(repo.name, passesWhiteBlackList(repo.name))
}

blobStore.blobStoreManager.browse().each { blobstore ->
    //check that this blobstore is not a group (3.15.0+)
    if (blobstore.getProperties().getOrDefault('groupable', true)) {
        //S3 stores currently cannot be analysed via this script, so ignore (3.12.0+)
        if (blobstore.getProperties().get("blobStoreConfiguration").type == "S3") {
            log.info("Ignoring blobstore {} as it is using S3", blobstore.getProperties().get("blobStoreConfiguration").name);
        } else {
            try {
                blobstoreName = blobstore.getProperties().get("blobStoreConfiguration").name
                blobStoreDirectories[blobstoreName] = blobstore.getProperties().get("absoluteBlobDir").toFile()
            }
            catch (Exception ex) {
                log.warn('Unable to add blobstore {} of type {}: {}', blobstore.getProperties().get("blobStoreConfiguration").name, blobstore.getProperties().get("blobStoreConfiguration").type, ex.getMessage())
                log.info('details: {}', blobstore.getProperties())
            }
        }
    } else {
        log.info("Ignoring blobstore {} as it is a group store", blobstore.getProperties().get("blobStoreConfiguration").name);
    }
}

log.info('Blob Storage scan STARTED.')
blobStoreDirectories.each { blobStore ->
    Path contentDir = blobStore.value.toPath().resolve('content')
    log.info('Scanning blobstore {}, root dir {}, content dir {}', blobStore.key, blobStore.value.absolutePath, contentDir)

    BlobStatistics blobStat = new BlobStatistics()

    Set<String> unmapped = new HashSet<>()
    storeRepositoryLookup[blobStore.value.getName()].each { key, value ->
        if (value) {
            blobStat.repositories.put(key, new RepoStatistics())
        } else {
            unmapped.add(key)
        }
    }

    def blobstoreDir = new File(blobStore.value.path)
    def includePattern = "glob:**${SEP}${blobstoreDir.getName()}${SEP}content${SEP}**${SEP}*.properties"
    PathMatcher INCLUDE_MATCHER = FileSystems.getDefault().getPathMatcher(includePattern)
    log.info("Looking for blob properties files matching: ${includePattern}")
    contentDir.eachFileRecurse(FILES) { p ->
        if (!EXCLUDE_MATCHER.matches(p) && INCLUDE_MATCHER.matches(p)) {
            File propertiesFile = p.toFile()
            def properties = new Properties()
            try {
                propertiesFile.withInputStream { is ->
                    properties.load(is)
                }
            } catch (FileNotFoundException ex) {
                log.warn("File not found '{}', skipping", propertiesFile.getCanonicalPath())
            } catch (Exception e) {
                log.error('Unable to process {}', propertiesFile.getAbsolutePath(), e)
                throw e
            }
            collectMetrics(blobStat, unmapped, properties, propertiesFile)
        }
    }
    blobStatCollection.put(blobStore.value.getName(), blobStat)
}

def getGB(long value) {
    return (value / 1024 / 1024 / 1024).setScale(2, RoundingMode.HALF_UP)
}

blobStatCollection.each() { blobStoreName, blobStat ->
    RepoStatistics directPath = blobStat.repositories.remove('SYSTEM:direct-path')
    if (directPath != null) {
        log.info("Direct-Path size in blobstore {}: {} - reclaimable: {}", blobStoreName, directPath.totalBytes, directPath.reclaimableBytes)
    }
    blobStat.totalBlobStoreGB = getGB(blobStat.totalBlobStoreBytes)
    blobStat.totalReclaimableGB = getGB(blobStat.totalReclaimableBytes)
    blobStat.repositories = blobStat.repositories.toSorted { a, b -> b.value.totalBytes <=> a.value.totalBytes }
    blobStat.repositories.each() { k, v ->
        blobStat.repositories."$k".totalGB = getGB(v.totalBytes)
        blobStat.repositories."$k".reclaimableGB = getGB(v.reclaimableBytes)
    }
}

return JsonOutput.toJson(blobStatCollection.findAll { a, b -> b.repositories.size() > 0 }.toSorted { a, b -> b.value.totalBlobStoreBytes <=> a.value.totalBlobStoreBytes })