package utils;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * jgit
 * Created by sjk on 12/26/16.
 */
public class GitStatistic {
    public static void main(String[] args) throws IOException, GitAPIException {
        statistic("/Users/sjk/g/tensorflow/.git");
    }

    public static void statistic(String path) throws IOException, GitAPIException {
        File home = new File(path);

        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        Repository repository = builder.setGitDir(home)
                .readEnvironment()
                .findGitDir()
                .build();
        Git git = new Git(repository);

        Iterable<RevCommit> log = git.log().call();
        Map<String, AtomicInteger> map = new ConcurrentSkipListMap<>();
        for (RevCommit rc : log) {
            String email = rc.getAuthorIdent().getEmailAddress();
            String[] tp = email.split("@");
            if (tp.length > 1) {
                String company = tp[1];
                if (map.containsKey(company)) {
                    map.get(company).incrementAndGet();
                } else {
                    map.put(company, new AtomicInteger(1));
                }
            }
        }

        for (Map.Entry<String, AtomicInteger> entry : map.entrySet()) {
            System.out.println(entry.getKey() + "  " + entry.getValue());
        }
    }
}
