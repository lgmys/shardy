<script setup lang="ts">
import { onMounted, ref, watchEffect } from 'vue'

const query = defineModel<string>()

const resultsRef = ref<{ items: Record<string, string>[]; columns: string[] }>()

const loadEntries = async (query = 'select * from logs') => {
  const response = await fetch('http://localhost:3000/search', {
    method: 'post',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      query,
    }),
  })
  resultsRef.value = await response.json()
}

onMounted(loadEntries)

watchEffect(async () => {
  await loadEntries(query.value)
})
</script>

<template>
  <input v-model="query" />

  <table>
    <thead>
      <tr>
        <th v-for="column in resultsRef?.columns" :key="column">
          {{ column }}
        </th>
      </tr>
    </thead>

    <tbody>
      <tr v-for="entry in resultsRef?.items" :key="entry.id">
        <td v-for="column in resultsRef?.columns" :key="column">
          {{ entry[column] }}
        </td>
      </tr>
    </tbody>
  </table>
</template>

<style scoped></style>
