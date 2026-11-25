package cl.duoc.barriodigital.admin.service;

import cl.duoc.barriodigital.admin.dto.CreateKafkaTopicDto;
import cl.duoc.barriodigital.admin.dto.KafkaClusterInfoDto;
import cl.duoc.barriodigital.admin.dto.KafkaConsumerGroupDto;
import cl.duoc.barriodigital.admin.dto.KafkaTopicInfoDto;

import java.util.List;

public interface KafkaAdminService {
    List<KafkaTopicInfoDto> listTopics();
    void createTopic(CreateKafkaTopicDto dto);
    void deleteTopic(String topicName);
    List<KafkaConsumerGroupDto> listConsumerGroups();
    KafkaClusterInfoDto getClusterInfo();
}
